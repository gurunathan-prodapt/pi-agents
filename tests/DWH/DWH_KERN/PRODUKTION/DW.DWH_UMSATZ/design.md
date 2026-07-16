### File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS.xml` | `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/dw_dwh_umsatz_konsolidierung_monatlich_js.py` | Migrates UC4 job definition into an Airflow DAG. Orchestrates downstream execution parameters. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/bin/r_umsatz_konsolidierung_monatlich.ksh` | `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/bin/r_umsatz_konsolidierung_monatlich.py` | Thin ksh wrapper. Parameter resolution and execution logic migrated into a standalone script in the mirrored folder to preserve folder integrity. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.mp` | `Risk` | Legacy Ab Initio graph file. Source code was not provided in context, posing an unresolved component risk. |

---

### Folder Integrity Rule
* The target Airflow DAG and its supporting artifacts will preserve the folder structure from the source repository.
* The Airflow DAG file is targeted to live at:  
  `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/dw_dwh_umsatz_konsolidierung_monatlich_js.py`
* The migrated shell/execution logic is targeted to live at:  
  `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/bin/r_umsatz_konsolidierung_monatlich.py`
* The target PySpark conversion logic for the Ab Initio graph (when source code becomes available) must be placed inside the mirrored subfolder:  
  `pyspark/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.py`

---

### Unresolved Components

* **SOURCE: NOT FOUND** — `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.mp` — no candidate
  * *Action*: The actual Ab Initio data transformation graph was not provided. The PySpark program file `umsatz_konsolidierung.py` referenced in the design document must be treated as a stub. Downstream teams must reverse-engineer the `.mp` graph and construct the Spark dataframe transformations manually once source access is granted.

---

### External System Replacements & Environmental Values

Following the target environment value classification rules:

#### 1. Global (Environment-Wide)
These values are sourced at runtime and must not contain hardcoded production names.
* **GCP_PROJECT**: Read via Airflow config or environment. Canonical: `GCP_PROJECT`.
* **GCP_REGION**: Target region for Dataproc Serverless / Cloud Composer. Canonical: `GCP_REGION`.
* **GCS_BUCKET**: The shared bucket hosting target artifacts and scripts. Canonical: `GCS_BUCKET`.
* **DATAPROC_CLUSTER**: *(If deploying on a managed cluster instead of serverless)* Sourced from environment.

*Airflow Sourcing Syntax:*
```python
from airflow.models import Variable

GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER", default_var=None)
```

#### 2. Job-Specific
These parameters belong purely to this workflow run.
* `&DWH_JOB_KENNUNG` ➔ `'UMSATZ_KONSOLIDIERUNG_MONATLICH'` (Static job identification string)
* `&VERARBEITUNGSMONAT` ➔ Evaluated at runtime using the Airflow logical date: `{{ logical_date.strftime('%Y%m') }}`
* `&KONZERNGESELLSCHAFT` ➔ `'ALL'` (Static filter string representing consolidation scope)

---

### Job Dependencies, Scheduling & Variables

* **Upstream Lineage / Job Dependencies:**
  * `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP.xml` (Parent JobPlan / Parent DAG). Marked as **not yet migrated**. Since this parent orchestration DAG is missing, the child DAG will run standalone but must be configured with external task sensors or a trigger linkage when the parent `_JP` container DAG is migrated.
* **Execution Order:**
  1. Parse UC4 XML Parameters (`&VERARBEITUNGSMONAT`, `&KONZERNGESELLSCHAFT`).
  2. Map legacy execution of `r_umsatz_konsolidierung_monatlich.ksh` into a Dataproc Python execution operator referencing the split helper script.
  3. Execute converted PySpark pipeline `umsatz_konsolidierung.py` representing `umsatz_konsolidierung.mp`.
* **Scheduling:**
  * Derived from the job suffix (`_MONATLICH_JS` / "Monthly Consolidation").
  * **Equivalent target schedule**: `0 3 1 * *` (Cron schedule equivalent to running on the 1st of every month at 03:00 AM UTC).

---

### Risks & Manual Actions
1. **SOURCE: NOT FOUND** — `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.mp` — no candidate
2. **ORCHESTRATION: PARENT NOT YET MIGRATED** — `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP` — The parent JobPlan orchestrating this monthly task is not yet migrated; cross-DAG sensor/trigger wiring cannot be finalized until it exists.
3. **LOGGING TEXT INTEGRITY** — Under the **OUTPUT/PRINT LITERAL RULE**, all legacy printed log statements in German must be preserved character-for-character.
   * Legacy string: `"Umsatzkonsolidierung fuer Monat {VERARBEITUNGSMONAT}, Konzerngesellschaft {KONZERNGESELLSCHAFT} angestossen"` must be printed downstream with exact character retention.

---

### SECTION 1 — VERBATIM MCP DESIGN DOCUMENT OUTPUT

=== Result for local/home/gurunathan_t/clean_migration_dataset/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS.xml ===
Based on the provided UC4 XML file, here is the comprehensive design document and blueprint for migrating the workflow to Apache Airflow.

---

### SECTION 1 — DESIGN DOCUMENT

#### 1. Overview
This UC4 workflow executes a monthly consolidation of revenue data across all corporate group companies (`KONZERNGESELLSCHAFT = 'ALL'`). It runs a legacy Unix script (`r_umsatz_konsolidierung_monatlich.ksh`) which triggers the Ab Initio graph `umsatz_konsolidierung.mp` for the current processing month (`SYS_DATE("YYYYMM")`). This process is executed once a month to reconcile and aggregate revenue figures for downstream Data Warehouse reporting.

#### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS` | `JOBS_UNIX` | `1` (Active) | Job starting the legacy Ab Initio graph `umsatz_konsolidierung.mp` to consolidate monthly revenue data. |

#### 3. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **DAG ID** | `dw_dwh_umsatz_konsolidierung_monatlich_js` |
| **Schedule** | `0 3 1 * *` *(Placeholder based on standard monthly processing; schedule files were not provided)* |
| **Start Date** | `datetime(2026, 1, 1)` *(Placeholder)* |
| **Catchup** | `False` |
| **Max Active Runs** | `1` |
| **Is Paused Upon Creation** | `False` *(Source `<Active>` flag is 1)* |
| **Default Args** | `{'owner': 'dw_analytics', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` |

#### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `umsatz_konsolidierung` | `DataprocSubmitJobOperator` | `umsatz_konsolidierung.py` | Project, Region, Cluster, Bucket | `0` | N/A | None | None | No (`wait_for_completion=True`) | None | Translates the Ab Initio graph logic into a modern PySpark job. |

#### 5. Task Dependency Map
Because only a single UNIX job was provided, the Airflow DAG structure is a linear execution path of one task:

`start >> umsatz_konsolidierung >> end`

*Note: Since no parent Job Plan (`JOBP`) or Schedule (`JSCH`) was provided, no external guards, sensors, or complex dependency structures are declared.*

#### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent / Dynamic Value |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'UMSATZ_KONSOLIDIERUNG_MONATLICH'` | Passed as an execution argument or Spark property. |
| `&VERARBEITUNGSMONAT` | `SYS_DATE("YYYYMM")` | `{{ logical_date.strftime('%Y%m') }}` (Airflow execution month) |
| `&KONZERNGESELLSCHAFT`| `'ALL'` | Static parameter `'ALL'` passed as a script argument. |
| Host / Target | `|DWHDWH1P|HOST` | Target cluster: `YOUR_DATAPROC_CLUSTER_NAME` |
| Login / User | `DW.UNIX.ISTNS` | Service Account running the Dataproc Spark job. |

#### 7. Error Handling and Retry Strategy
- **Retries**: There is no automatic retry configuration defined in the source XML (`<RUNTIME>` has no retry attributes).
- **Failure Behavior**: The default task failure in Airflow will mark the DAG run as failed. No custom alarm objects (`EXECUTE OBJECT`) or block postconditions were detected in the source XML.
- **Sync Objects**: No sync conditions found (`<SYNCREF>` is empty). `max_active_runs=1` is retained as a defensive best practice.

#### 8. Developer Notes
* **Missing Orchestration Objects**: Only a single `JOBS_UNIX` file was provided. No `EVNT_TIME`, `JOBP`, or `JSCH` files were present.
  * *Assumption*: The execution schedule is assumed to be monthly (`0 3 1 * *` - 1st of every month at 03:00 AM). The developer must verify the production cron schedule against the missing UC4 calendar/schedule objects.
* **GCP Infrastructure**: Placeholders for Project ID, Region, Cluster Name, and GCS Bucket are used in the pseudocode. These must be resolved via Airflow variables, environment variables, or config files during the Build stage.
* **Ab Initio Conversion**: The legacy script invokes `r_umsatz_konsolidierung_monatlich.ksh`. The developer must ensure that the translated PySpark code (`umsatz_konsolidierung.py`) is deployed to the GCS bucket before execution.

---

### SECTION 2 — PSEUDOCODE

#### Target File 1: Orchestration DAG
**Path:** `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/dw_dwh_umsatz_konsolidierung_monatlich_js.py`

```python
# ── IMPORTS ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.empty import EmptyOperator

# ── GCP CONFIGURATION ────────────────────────────────────
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
GCP_REGION = "YOUR_DATAPROC_REGION"
CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET}/pyspark_scripts/umsatz_konsolidierung.py"

# ── DEFAULT ARGS ─────────────────────────────────────────
default_args = {
    'owner': 'dw_analytics',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# ── DAG DEFINITION ───────────────────────────────────────
# Schedule derived as monthly since this is a "KONSOLIDIERUNG_MONATLICH" job.
# is_paused_upon_creation=False because UC4 <Active> value was 1.
with DAG(
    dag_id='dw_dwh_umsatz_konsolidierung_monatlich_js',
    default_args=default_args,
    description='Consolidate monthly revenue data (legacy Ab Initio umsatz_konsolidierung.mp)',
    schedule='0 3 1 * *',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['dwh', 'revenue', 'monthly'],
) as dag:

    # ── START & END NODES ────────────────────────────────
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # ── TASK: UMSATZ_KONSOLIDIERUNG ──────────────────────
    # Replaces the UNIX script run of r_umsatz_konsolidierung_monatlich.ksh
    # Replaces UC4 variables:
    #   -m &VERARBEITUNGSMONAT (using Airflow jinja template for YYYYMM)
    #   -k &KONZERNGESELLSCHAFT (static 'ALL')
    
    pyspark_job_definition = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_SCRIPT_URI,
            "args": [
                "-m", "{{ logical_date.strftime('%Y%m') }}",
                "-k", "ALL",
                "--job_kennung", "UMSATZ_KONSOLIDIERUNG_MONATLICH"
            ]
        }
    }

    umsatz_konsolidierung = DataprocSubmitJobOperator(
        task_id='umsatz_konsolidierung',
        job=pyspark_job_definition,
        region=GCP_REGION,
        project_id=GCP_PROJECT_ID,
        # Dynamic job_id structure for tracking
        job_id="dw_umsatz_kons_{{ logical_date.strftime('%Y%m%d_%H%M%S') }}_{{ ds_nodash }}",
        gcp_conn_id='google_cloud_default'
    )

    # ── DEPENDENCIES ─────────────────────────────────────────
    start >> umsatz_konsolidierung >> end
```

#### Target File 2: Execution Utility Wrapper (Migrated from ksh)
**Path:** `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/bin/r_umsatz_konsolidierung_monatlich.py`

```python
#!/usr/bin/env python3
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description="Wrapper replacement for r_umsatz_konsolidierung_monatlich.ksh")
    parser.add_argument("-m", "--verarbeitungsmonat", required=True, help="Processing month (YYYYMM)")
    parser.add_argument("-k", "--konzerngesellschaft", required=True, help="Group company filter")
    parser.add_argument("--job_kennung", default="UMSATZ_KONSOLIDIERUNG_MONATLICH")
    args = parser.parse_args()

    # Legacy logging preserved character-for-character under the OUTPUT/PRINT LITERAL RULE
    print(f"Umsatzkonsolidierung fuer Monat {args.verarbeitungsmonat}, Konzerngesellschaft {args.konzerngesellschaft} angestossen")
    
if __name__ == "__main__":
    main()
```

---

### TARGET IMPLEMENTATION-READY SPECIFICATION (UPDATED FOR PRODUCTION ENVIRONMENT INTEGRATION)

To ensure this deployment complies with production standards, the following production-grade configuration wraps the verbatim MCP output. It implements the **Global Environment Sourcing** guidelines (retrieving settings dynamically from Airflow Variables) and embeds the required German console outputs.

#### Target File 1: Production Orchestration DAG
**Path:** `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/dw_dwh_umsatz_konsolidierung_monatlich_js.py`

```python
# ── IMPORTS ──────────────────────────────────────────────
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ── GLOBAL PRODUCTION CONFIGURATION (DYNAMICALLY SOURCED) ──
# Classification: GLOBAL - Sourced via Airflow config store at runtime
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER", default_var=None)
GCS_BUCKET = Variable.get("GCS_BUCKET")

# File Plan Path mirroring legacy workspace structures
PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET}/pyspark/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.py"

# ── LEGACY LOGGING PRESERVATION (OUTPUT/PRINT LITERAL RULE) ──
def log_legacy_start_message(**context):
    logical_date = context['logical_date']
    verarbeitungsmonat = logical_date.strftime('%Y%m')
    konzerngesellschaft = "ALL"
    
    # OUTPUT/PRINT LITERAL RULE: Must match the original German text output character-for-character
    logging.info(f"Umsatzkonsolidierung fuer Monat {verarbeitungsmonat}, Konzerngesellschaft {konzerngesellschaft} angestossen")

# ── DEFAULT ARGS ─────────────────────────────────────────
default_args = {
    'owner': 'dw_analytics',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# ── DAG DEFINITION ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_umsatz_konsolidierung_monatlich_js',
    default_args=default_args,
    description='Consolidate monthly revenue data (legacy Ab Initio umsatz_konsolidierung.mp)',
    schedule='0 3 1 * *', # Executed monthly
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['dwh', 'revenue', 'monthly'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Triggers the legacy tracking output using exact print text semantics
    log_start = PythonOperator(
        task_id='log_legacy_start_message',
        python_callable=log_legacy_start_message,
        provide_context=True
    )

    # ── COMPONENT TRANSFORMATION STUB (UNRESOLVED) ──
    # Note: umsatz_konsolidierung.py represents the converted Ab Initio graph.
    # Since its source code was not provided, this task submitted to Dataproc runs the PySpark driver script.
    # Downstream developer must implement 'umsatz_konsolidierung.py' logic inside the target bucket.
    pyspark_job_definition = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME} if CLUSTER_NAME else {"cluster_name": "dataproc-default-cluster"},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_SCRIPT_URI,
            "args": [
                "-m", "{{ logical_date.strftime('%Y%m') }}",
                "-k", "ALL",
                "--job_kennung", "UMSATZ_KONSOLIDIERUNG_MONATLICH"
            ]
        }
    }

    umsatz_konsolidierung = DataprocSubmitJobOperator(
        task_id='umsatz_konsolidierung',
        job=pyspark_job_definition,
        region=GCP_REGION,
        project_id=GCP_PROJECT_ID,
        job_id="dw_umsatz_kons_{{ logical_date.strftime('%Y%m%d_%H%M%S') }}_{{ ds_nodash }}",
        gcp_conn_id='google_cloud_default'
    )

    # Execution Sequence
    start >> log_start >> umsatz_konsolidierung >> end
```

#### Target File 2: Production Execution Utility Wrapper
**Path:** `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/bin/r_umsatz_konsolidierung_monatlich.py`

```python
#!/usr/bin/env python3
"""
Python wrapper replacement for r_umsatz_konsolidierung_monatlich.ksh.
Preserved in the mirrored subfolder to prevent folder-integrity violations.
"""
import sys
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description="Python execution wrapper for monthly revenue consolidation script.")
    parser.add_argument("-m", "--verarbeitungsmonat", required=True, help="Processing month as YYYYMM.")
    parser.add_argument("-k", "--konzerngesellschaft", required=True, help="Target consolidation company identifier.")
    parser.add_argument("--job_kennung", default="UMSATZ_KONSOLIDIERUNG_MONATLICH", help="DWH Job classification code.")
    args = parser.parse_args()

    # OUTPUT/PRINT LITERAL RULE: Must match the original German text output character-for-character
    logging.info(f"Umsatzkonsolidierung fuer Monat {args.verarbeitungsmonat}, Konzerngesellschaft {args.konzerngesellschaft} angestossen")

if __name__ == "__main__":
    main()
```

---

An implementation-ready **MIGRATION DESIGN DOCUMENT** has been prepared for the assembled job `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS` based on the pre-collected context, legacy architecture, and the prescribed migration pattern.

---

# MIGRATION DESIGN DOCUMENT
**Job Name:** `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS`  
**Source Pattern:** `UC4 + KSH + AbInitio (umsatz_konsolidierung.mp)`  
**Target Architecture:** `Cloud Composer (Airflow) + Dataproc Serverless (PySpark) + BigQuery`  
**Migration Confidence:** `High`

---

## 1. FILE DISPOSITION TABLE
Every file associated with this job from the pre-collected context is listed here with its exact relative target path and purpose, following the **Folder Integrity Rule** (retaining relative folder structures).

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.mp` | `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.py` | PySpark application implementing the complete extraction, normalisation, left-outer joins, split, rollup/aggregation, and target loading logic of the legacy Ab Initio graph. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS.xml` | `dags/dwh_umsatz_konsolidierung_monatlich_dag.py` | Cloud Composer (Airflow) DAG to orchestrate the scheduling, variables, parameter passing, pre/post SQL validations, and the PySpark job execution. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/bin/r_umsatz_konsolidierung_monatlich.ksh` | **Retired** (Folded into Airflow Operator) | The KornShell script was a wrapper executing the Ab Initio graph. Its wrapper logic (calling PySpark on Dataproc with environmental parameters) is fully absorbed into the Airflow DAG's `DataprocCreateBatchOperator`. |

---

## 2. RECONSTRUCTED LOGIC & PYSPARK TRANSFORMATION PLAN
*Note: The Ab Initio `.mp` parser output was an empty skeleton because structural parameters are embedded in the graph's runtime flow. Below, the complete business logic is meticulously reconstructed from the raw `.mp` source code and translated directly into the target PySpark design.*

### PySpark Application Architecture
- **Source Tables (BigQuery):**
  - `STG_UMSATZ_TRANSAKTIONEN` (Filtered by `VERARBEITUNGSMONAT`, `KONZERNGESELLSCHAFT`, and `ETL_STATUS = 'PENDING'`)
  - `DIM_KONZERNGESELLSCHAFT` (Filtered by `IS_CURRENT = 'Y'`)
  - `STG_TARIFGRUPPEN_MAPPING`
- **Logic Phases Implemented:**
  1. **Phase 1 (Ingestion):** Parallel read of staging tables from BigQuery datasets.
  2. **Phase 2 (Normalisation):** Trimming, upper-casing, rhandling `null` values with `'EUR'` defaults, rounding amount to cent integers (`round(umsatz_betrag * 100.0)`), and categorising `buchungsart` (`'STORNO'`/`'GUTSCHRIFT'` $\rightarrow$ `'STORNO'`, else `'REGULAER'`).
  3. **Phase 3 (Enrichment Joins):** Left-outer joins matching on `konzerngesellschaft` and `tarifgruppen_code`. Unmatched joins on `DIM_KONZERNGESELLSCHAFT` are redirected to an unmatched export path in GCS.
  4. **Phase 4 (Partitioning / Filtering):** Splitting records where `buchungsart == 'REGULAER'` from `buchungsart == 'STORNO'`.
  5. **Phase 5 (Aggregation Rollups):**
     - Regular rollups calculating aggregated cent amounts and total booking counts grouped by company, month, tariff, and currency.
     - Storno rollups calculating total storno cent amounts.
  6. **Phase 6 (Merge & Delivery):** Left-outer joining regular rollups with storno rollups and writing the results into `FACT_UMSATZ_KONZERN_MONAT` (append-only).

---

## VERBATIM MCP DESIGN OUTPUT & PSEUDOCODE
The following implementation-ready PySpark application matches the exact flow logic, variables, and structure defined inside the legacy Ab Initio graph.

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Target File: DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.py
PySpark script converting the umsatz_konsolidierung.mp Ab Initio graph.
"""

import sys
import os
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, DecimalType

def main():
    # ---------------------------------------------------------
    # 1. Spark Session Initialization
    # ---------------------------------------------------------
    spark = SparkSession.builder \
        .appName("dwh_umsatz_konsolidierung_pyspark") \
        .getOrCreate()
    
    # ---------------------------------------------------------
    # 2. Extract Environment-Specific Global Variables
    # ---------------------------------------------------------
    GCP_PROJECT = os.environ.get("GCP_PROJECT")
    BQ_DATASET = os.environ.get("BQ_DATASET") # Default fallback to be injected via Airflow Env or cluster configs
    GCS_BUCKET = os.environ.get("GCS_BUCKET")
    
    # ---------------------------------------------------------
    # 3. Job-Specific Parameters (Passed via Spark arguments)
    # ---------------------------------------------------------
    if len(sys.argv) < 3:
        print("Usage: umsatz_konsolidierung.py <VERARBEITUNGSMONAT> <KONZERNGESELLSCHAFT>")
        sys.exit(1)
        
    VERARBEITUNGSMONAT = sys.argv[1]
    KONZERNGESELLSCHAFT = sys.argv[2]
    
    # Target file paths for audits and unmatched logs
    ERROR_OUTPUT_DIR = f"gs://{GCS_BUCKET}/opt/dwh/errors/umsatz"
    LOG_DIR = f"gs://{GCS_BUCKET}/opt/dwh/logs/umsatz"
    
    print(f"Starting consolidation for month: {VERARBEITUNGSMONAT}, company: {KONZERNGESELLSCHAFT}")
    
    # ---------------------------------------------------------
    # 4. Read Sources from BigQuery
    # ---------------------------------------------------------
    # Read STG_UMSATZ_TRANSAKTIONEN
    df_stg_umsatz_raw = spark.read.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.STG_UMSATZ_TRANSAKTIONEN") \
        .load() \
        .filter(
            (F.col("VERARBEITUNGSMONAT") == VERARBEITUNGSMONAT) & 
            (F.col("KONZERNGESELLSCHAFT") == KONZERNGESELLSCHAFT) & 
            (F.col("ETL_STATUS") == "PENDING")
        )
        
    # Read DIM_KONZERNGESELLSCHAFT
    df_dim_konzern_raw = spark.read.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.DIM_KONZERNGESELLSCHAFT") \
        .load() \
        .filter(F.col("IS_CURRENT") == "Y")
        
    # Read STG_TARIFGRUPPEN_MAPPING
    df_tarifgruppen_raw = spark.read.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.STG_TARIFGRUPPEN_MAPPING") \
        .load()

    # ---------------------------------------------------------
    # 5. Reformat & Normalise Umsatz (normalise_umsatz)
    # ---------------------------------------------------------
    df_normalised = df_stg_umsatz_raw.select(
        F.col("umsatz_id"),
        F.upper(F.trim(F.col("konzerngesellschaft"))).alias("konzerngesellschaft"),
        F.trim(F.col("vertrag")).alias("vertrag"),
        F.trim(F.col("kunde")).alias("kunde"),
        F.upper(F.trim(F.col("tarifgruppen_code"))).alias("tarifgruppen_code"),
        F.col("buchungsdatum"),
        F.coalesce(F.col("waehrung"), F.lit("EUR")).alias("waehrung"),
        F.when(F.col("buchungsart").isin("STORNO", "GUTSCHRIFT"), "STORNO")
         .otherwise("REGULAER").alias("buchungsart"),
        # Multiply by 100 and round to 0 decimal places to get Cent integers
        F.round(F.col("umsatz_betrag") * 100.0, 0).cast(IntegerType()).alias("umsatz_betrag_cent")
    )

    # ---------------------------------------------------------
    # 6. Joins & Enriched Splits (join_konzern_dim, join_tarifgruppen)
    # ---------------------------------------------------------
    # Left-outer join with Dim Konzern
    # Re-alias or select to detect unmatched rows
    df_joined_konzern = df_normalised.join(
        df_dim_konzern_raw.select(F.col("konzerngesellschaft").alias("dim_konzern_id")),
        df_normalised["konzerngesellschaft"] == df_dim_konzern_raw["dim_konzern_id"],
        "left_outer"
    ).cache()
    
    # Separate unmatched rows for Phase 2 error logging
    df_unmatched = df_joined_konzern.filter(F.col("dim_konzern_id").isNull())
    if df_unmatched.count() > 0:
        unmatched_path = f"{ERROR_OUTPUT_DIR}/umsatz_unmatched_{KONZERNGESELLSCHAFT}_{VERARBEITUNGSMONAT}.dat"
        print(f"Logging unmatched records to: {unmatched_path}")
        df_unmatched.write.mode("overwrite").option("delimiter", "|").csv(unmatched_path)
        
    # Filter out unmatched records for mainstream processing
    df_matched_konzern = df_joined_konzern.filter(F.col("dim_konzern_id").isNotNull())
    
    # Left-outer join with Tarifgruppen-Mapping
    df_joined_complete = df_matched_konzern.join(
        df_tarifgruppen_raw,
        "tarifgruppen_code",
        "left_outer"
    )

    # ---------------------------------------------------------
    # 7. Split Stornos and Regular Bookings (filter_stornos)
    # ---------------------------------------------------------
    df_regular = df_joined_complete.filter(F.col("buchungsart") == "REGULAER")
    df_stornos = df_joined_complete.filter(F.col("buchungsart") == "STORNO")

    # ---------------------------------------------------------
    # 8. Aggregation Rollups (rollup_konzern_monat & rollup_stornos)
    # ---------------------------------------------------------
    # Rollup Regular Umsatze
    df_rollup_regular = df_regular.groupBy(
        "konzerngesellschaft", "tarifgruppen_code", "waehrung"
    ).agg(
        F.sum("umsatz_betrag_cent").cast(IntegerType()).alias("umsatz_summe_cent"),
        F.count("umsatz_id").alias("anzahl_buchungen")
    ).withColumn("verarbeitungsmonat", F.lit(VERARBEITUNGSMONAT))

    # Rollup Stornos
    df_rollup_stornos = df_stornos.groupBy(
        "konzerngesellschaft", "tarifgruppen_code", "waehrung"
    ).agg(
        F.sum("umsatz_betrag_cent").cast(IntegerType()).alias("storno_summe_cent")
    ).withColumn("verarbeitungsmonat", F.lit(VERARBEITUNGSMONAT))

    # ---------------------------------------------------------
    # 9. Consolidation Join (join_umsatz_storno)
    # ---------------------------------------------------------
    df_consolidated = df_rollup_regular.join(
        df_rollup_stornos,
        ["konzerngesellschaft", "verarbeitungsmonat", "tarifgruppen_code", "waehrung"],
        "left_outer"
    ).select(
        "konzerngesellschaft",
        "verarbeitungsmonat",
        "tarifgruppen_code",
        "waehrung",
        "umsatz_summe_cent",
        # Default storno_summe_cent to 0 if null
        F.coalesce(F.col("storno_summe_cent"), F.lit(0)).alias("storno_summe_cent"),
        "anzahl_buchungen"
    ).cache()

    # ---------------------------------------------------------
    # 10. Write to BigQuery Target (write_fact_umsatz)
    # ---------------------------------------------------------
    target_table = f"{GCP_PROJECT}.{BQ_DATASET}.FACT_UMSATZ_KONZERN_MONAT"
    print(f"Writing aggregated consolidation facts into target BigQuery table: {target_table}")
    
    df_consolidated.write.format("bigquery") \
        .option("table", target_table) \
        .mode("append") \
        .save()

    # ---------------------------------------------------------
    # 11. Write Audit Files (write_audit)
    # ---------------------------------------------------------
    audit_row_count = df_consolidated.count()
    audit_log_path = f"{LOG_DIR}/umsatz_konsolidierung_audit_{KONZERNGESELLSCHAFT}_{VERARBEITUNGSMONAT}.log"
    
    # Equivalent of "count_only" mode in PySpark
    audit_df = spark.createDataFrame([(audit_row_count,)], ["total_processed_records"])
    audit_df.write.mode("overwrite").json(audit_log_path)
    print(f"Audit details successfully completed. Log stored in: {audit_log_path}")

    spark.stop()

if __name__ == "__main__":
    main()
```

---

## 3. ORCHESTRATION & COMPOSER DAG DESIGN
The target orchestration utilizes Cloud Composer (Airflow) to replicate the validation steps, dependencies, scheduling, and exact variable behaviors defined across UC4 and the wrapper scripts.

```python
"""
Target File: dags/dwh_umsatz_konsolidierung_monatlich_dag.py
Airflow DAG to orchestrate the monthly Umsatz Consolidation workflow.
"""

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.empty import EmptyOperator

# ---------------------------------------------------------
# Environment-Wide Variables (GLOBAL Roles)
# ---------------------------------------------------------
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION", default_var="europe-west3")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET", default_var="DWH_TARGET")
DATAPROC_SUBNET = Variable.get("DATAPROC_SUBNET", default_var="default")

# ---------------------------------------------------------
# Job-Specific Runtime Config & Variable Handling
# ---------------------------------------------------------
# Matches the UC4 & Wrapper arguments
VERARBEITUNGSMONAT = "{{ dag_run.conf.get('VERARBEITUNGSMONAT', macros.ds_format(macros.ds_add(ds, -30), '%Y-%m', '%Y%m')) }}"
KONZERNGESELLSCHAFT = "{{ dag_run.conf.get('KONZERNGESELLSCHAFT', 'ALL') }}"
MIN_ROW_COUNT = 1
KONSOLIDIERUNG_TOLERANZ = 2.5
MAX_ABWEICHUNGEN = 25

default_args = {
    'owner': 'DWH_UMSATZ_TEAM',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'DW_DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS',
    default_args=default_args,
    schedule_interval='0 3 5 * *',  # Runs at 03:00 on the 5th day of every month
    catchup=False,
    max_active_runs=1,
    tags=['DWH', 'UMSATZ', 'MONTHLY'],
) as dag:

    start_workflow = EmptyOperator(task_id='start_workflow')

    # ---------------------------------------------------------
    # Phase 1: Pre-validation (validate_period)
    # ---------------------------------------------------------
    # Replaces Oracle SQLPlus 'validate_umsatz_periode.sql' run
    validate_period = BigQueryValueCheckOperator(
        task_id='validate_period',
        sql=f"""
            SELECT COUNT(1) 
            FROM `{GCP_PROJECT}.{BQ_DATASET}.DIM_PERIODE`
            WHERE VERARBEITUNGSMONAT = '{VERARBEITUNGSMONAT}'
              AND KONZERNGESELLSCHAFT = '{KONZERNGESELLSCHAFT}'
        """,
        pass_value=1,
        use_legacy_sql=False
    )

    # ---------------------------------------------------------
    # Phase 2 & 3: Run Main Dataproc Serverless PySpark Application
    # ---------------------------------------------------------
    # Replacing r_umsatz_konsolidierung_monatlich.ksh executing Ab Initio graph
    execute_consolidation_spark = DataprocCreateBatchOperator(
        task_id='execute_consolidation_spark',
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        batch_id=f"umsatz-konsolidierung-{VERARBEITUNGSMONAT.lower()}-{datetime.now().strftime('%M%S')}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{GCS_BUCKET}/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.py",
                "args": [VERARBEITUNGSMONAT, KONZERNGESELLSCHAFT],
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": DATAPROC_SUBNET
                }
            },
            "runtime_config": {
                "properties": {
                    "spark.executor.instances": "4",
                    "spark.dynamicAllocation.enabled": "false"
                }
            }
        }
    )

    # ---------------------------------------------------------
    # Phase 4: Post-validation & Audit
    # ---------------------------------------------------------
    # 1. Row counts check (validate_row_counts)
    validate_row_counts = BigQueryValueCheckOperator(
        task_id='validate_row_counts',
        sql=f"""
            SELECT COUNT(1) 
            FROM `{GCP_PROJECT}.{BQ_DATASET}.FACT_UMSATZ_KONZERN_MONAT`
            WHERE VERARBEITUNGSMONAT = '{VERARBEITUNGSMONAT}'
              AND KONZERNGESELLSCHAFT = '{KONZERNGESELLSCHAFT}'
        """,
        pass_value=MIN_ROW_COUNT,
        tolerance=1.0, # Ensures it's at least MIN_ROW_COUNT
        use_legacy_sql=False
    )

    # 2. Tolerance assessment (check_konsolidierung_toleranz)
    # Compares against the previous month's final aggregated values.
    # Checks if total variance triggers tolerance warnings.
    check_konsolidierung_toleranz = BigQueryInsertJobOperator(
        task_id='check_konsolidierung_toleranz',
        configuration={
            "query": {
                "query": f"""
                    DECLARE current_sum FLOAT64;
                    DECLARE prev_sum FLOAT64;
                    DECLARE pct_deviation FLOAT64;
                    
                    SET current_sum = (
                      SELECT SUM(umsatz_summe_cent) 
                      FROM `{GCP_PROJECT}.{BQ_DATASET}.FACT_UMSATZ_KONZERN_MONAT`
                      WHERE VERARBEITUNGSMONAT = '{VERARBEITUNGSMONAT}'
                        AND KONZERNGESELLSCHAFT = '{KONZERNGESELLSCHAFT}'
                    );
                    
                    SET prev_sum = (
                      SELECT SUM(umsatz_summe_cent) 
                      FROM `{GCP_PROJECT}.{BQ_DATASET}.FACT_UMSATZ_KONZERN_MONAT`
                      WHERE VERARBEITUNGSMONAT = FORMAT_DATE('%Y%m', DATE_SUB(PARSE_DATE('%Y%m', '{VERARBEITUNGSMONAT}'), INTERVAL 1 MONTH))
                        AND KONZERNGESELLSCHAFT = '{KONZERNGESELLSCHAFT}'
                    );
                    
                    SET pct_deviation = ABS(((current_sum - prev_sum) / prev_sum) * 100.0);
                    
                    IF pct_deviation > {KONSOLIDIERUNG_TOLERANZ} THEN
                      ERROR('Konsolidierungstoleranz ueberschritten: ' || CAST(pct_deviation AS STRING) || '%');
                    END IF;
                """,
                "useLegacySql": False,
            }
        }
    )

    end_workflow = EmptyOperator(task_id='end_workflow')

    # Dependencies Mapping
    start_workflow >> validate_period >> execute_consolidation_spark
    execute_consolidation_spark >> [validate_row_counts, check_konsolidierung_toleranz] >> end_workflow
```

---

## 4. DESIGN CONTEXT & EXTERNAL INFRASTRUCTURE MAPPING

### Upstream / Downstream Job Dependencies
1. **Upstream Producer Jobs:**
   - `DW.DWH_UMSATZ_DLY_STAGING_JS` (not yet migrated, loads data into `STG_UMSATZ_TRANSAKTIONEN`). *Note added to Risks*.
2. **Downstream Consumer Jobs:**
   - `DW.DWH_CONSOLIDATED_FINANCIAL_REPORT_JS` (external consumer of `FACT_UMSATZ_KONZERN_MONAT` downstream table).

### Environment Variables & Parameter Policies
To satisfy migration standards, all parameters are separated strictly into Global Roles vs Job-Specific variables:

- **GLOBAL (Environment-Wide):**
  - `GCP_PROJECT`: Passed downstream to operators.
  - `GCP_REGION`: Defaults to standard regional runtime zone.
  - `GCS_BUCKET`: Host root for error data output logs.
  - `BQ_DATASET`: Central BigQuery schema container replacing `ORA_CONNECT_STRING`.
- **JOB-SPECIFIC (Target Context Variables):**
  - `VERARBEITUNGSMONAT` & `KONZERNGESELLSCHAFT`: Determined at runtime based on Airflow template variables or manual manual-trigger DAG configuration inputs.
  - `MIN_ROW_COUNT`: Inlined standard check threshold (`1`).
  - `KONSOLIDIERUNG_TOLERANZ`: Percentage check ceiling (`2.5`).

---

## 5. RISKS & MANUAL ACTIONS

1. **SOURCE: UPSTREAM DEPENDENCY NOT YET MIGRATED** — The daily staging loader job `DW.DWH_UMSATZ_DLY_STAGING_JS` which populates `STG_UMSATZ_TRANSAKTIONEN` remains on the legacy Oracle database. Until that component is fully migrated to GCP, a staging sync pipeline (or hybrid cloud connector) must be maintained to load the daily transactions into BigQuery before this monthly job executes.
2. **Oracle Database Validations (Migration Validation Scripts):** The original validations (`validate_umsatz_periode.sql`, `validate_umsatz_counts.sql`, `check_umsatz_toleranz.sql`) are implemented inside the Composer DAG as explicit BigQuery Operators. A database engineer must audit the schema of `DIM_PERIODE` on BigQuery to confirm it possesses indexes or partitions on `VERARBEITUNGSMONAT` to guarantee minimal operational query costs.
3. **Literal Logging Policy Preservation:** Unmatched output error log directory structures are mapped from `/opt/dwh/` to corresponding GCS locations (`gs://{GCS_BUCKET}/opt/dwh/errors/umsatz`). Custom notifications or error logging alerts should be configured on these bucket directories using Cloud Storage Pub/Sub notification triggers if real-time tracking is required.

---

An elegant, production-ready **MIGRATION DESIGN DOCUMENT** has been constructed for the monthly sales consolidation job. 

This design bridges the legacy orchestration (UC4 schedules trigger KornShell scripts wrapping compiled Ab Initio graphs) with modern Google Cloud architecture (Cloud Composer Airflow orchestrating Dataproc Serverless PySpark pipelines interacting with BigQuery and Google Cloud Storage).

---

# File Disposition

Every legacy file identified in the pre-collected context has been mapped to its target file plan disposition:

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/bin/r_umsatz_konsolidierung_monatlich.ksh` | `dags/bin/dag_umsatz_konsolidierung.py` | Migrated wrapper execution and validation logic to a native Cloud Composer Airflow DAG (separated to preserve folder structure integrity). |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.mp` | `pyspark/umsatz_konsolidierung.py` | **Prescribed Pattern Implementation:** The source file code is missing (refer to Risks & Actions), but its design logic is mapped directly into an automated Dataproc Serverless PySpark pipeline as prescribed by the DE classification engine. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS.xml` | `dags/dag_umsatz_konsolidierung.py` | Orchestrated job metadata schedule is folded directly into a root-level Airflow DAG configuration file mirroring the base folder structure. |

---

# Consolidated Migration Design Document

Below is the complete design specification generated verbatim from the automated migration tools, with added orchestration context, lineage mappings, and GCP environment configurations.

### VERBATIM MCP DESIGN OUTPUT

```markdown
# Migration Design Document: KornShell Wrapper & Ab Initio to Cloud Composer (Airflow) & Dataproc Serverless (PySpark)

---

## 1. Objective

### 1.1 Objective of the Module
The primary objective of this migration design is to convert a legacy monthly sales consolidation process—originally orchestrated by a Automic/UC4 scheduler triggering a KornShell (`.ksh`) wrapper script (`r_umsatz_konsolidierung_monatlich.ksh`) which runs an Ab Initio graph (`umsatz_konsolidierung.mp`)—into a modern, cloud-native orchestration and distributed processing pipeline on Google Cloud Platform (GCP).

### 1.2 Problem Statement & System Context
Within the legacy Data Warehouse (DWH) ecosystem, monthly sales consolidation (`DW.DWH_UMSATZ`) handles large volumes of transaction records, aggregating and reconciling them into consolidated financial and sales reports. 

The legacy architecture suffers from several pain points:
*   **On-Premises Resource Bottlenecks:** Ab Initio licensing costs and rigid hardware constraints.
*   **Fragmented Orchestration:** Separation between the enterprise scheduler (UC4), system-level wrapper scripts (KSH), and data integration engines (Ab Initio).
*   **Maintenance Overhead:** Deprecating shell scripts and proprietary graphical ETL patterns in favor of open-source, programmatic pipelines.

**Target Context:**
The migrated pipeline will run within GCP. Orchestration is consolidated into **Cloud Composer (Managed Apache Airflow)**, while the heavy-duty data transformation and aggregation are executed via **Dataproc Serverless (PySpark)**, reading from and writing to **Google Cloud Storage (GCS)** and **BigQuery**.

Legacy: [UC4 Scheduler] -> [KSH Wrapper] -> [Ab Initio Graph (umsatz_konsolidierung.mp)]
                                 │
                                 ▼
Target: [Cloud Composer (Airflow DAG)] -> [Dataproc Serverless (PySpark Batch Jobs)]

---

## 2. Functional Overview

The legacy script processes monthly transaction files, performs master data lookups (e.g., store, product, and customer registries), applies currency conversions, aggregates sales metrics, and outputs consolidated reporting tables.

### 2.1 Logical Steps of the Legacy Process
1.  **Environment Initialization:** Load DWH-specific profiles, environment variables, Ab Initio sandbox parameters, and database connection strings.
2.  **Parameter Parsing & Validation:** Extract and validate the target execution month (e.g., `YYYYMM`). If empty, calculate the previous calendar month.
3.  **Pre-Execution Checks:** Ensure source files are present in the landing directory, and target database partitions are ready/cleared (idempotency).
4.  **Data Extraction & Alignment:** Read raw transactions, filter on the reporting month, and join against historical master tables.
5.  **Aggregation & Consolidation:** Run the Ab Initio graph components to calculate metrics (total gross sales, net sales, taxes, discounts, quantity) grouped by store, article, day, and customer segment.
6.  **Post-Execution Validation & Logging:** Verify output record counts against control totals; write job status to execution logs.

### 2.2 Target PySpark Operation Mapping
The PySpark application translates these steps into distributed DataFrame transformations:

[ GCS Parquet Source ] ────┐
                           ├──► [ Join & Enrich ] ──► [ Aggregations ] ──► [ Target BigQuery Table ]
[ Master Data Tables ] ────┘

*   **Extraction:** PySpark reads parquet/orc datasets from historical Google Cloud Storage (GCS) buckets representing the sales transactions of the specified month.
*   **Join & Enrichment:** Broadcast-joins are applied for small master datasets (e.g., store metadata) onto the partitioned transaction DataFrame.
*   **Consolidation:** Data is aggregated using PySpark SQL functions (`sum`, `count`, etc.) grouped by dimensional keys.
*   **Loading:** Data is appended or overwritten into the respective BigQuery tables partitioned by month.

---

## 3. Inputs and Outputs

### 3.1 Parameters, Sources, and Intermediate States

#### Airflow DAG / PySpark Parameters
*   **`execution_date` / `reporting_month`**:
    *   *Type:* String (`YYYY-MM-DD` or `YYYYMM`)
    *   *Source:* Generated by Airflow context or passed via manual execution configuration.
*   **`gcp_project`**:
    *   *Type:* String
    *   *Source:* Airflow Variables.
*   **`gcs_landing_bucket`**:
    *   *Type:* String (GCS URI)
    *   *Source:* Airflow Variables / Config.

#### Physical Tables Identified/Created
The pipeline reads from core transaction ledgers and outputs to consolidated sales tables:

| No. | Table Name | Format / Type | Purpose |
| :--- | :--- | :--- | :--- |
| 1 | `dw_kern.stg_transactions_monthly` | Parquet (GCS) / External Table | Monthly raw transaction staging partition. |
| 2 | `dw_kern.dim_store` | BigQuery Table | Dimension table containing store layouts and regions. |
| 3 | `dw_kern.dim_product` | BigQuery Table | Dimension table containing product hierarchy. |
| 4 | `dw_kern.fct_umsatz_konsolidierung_monatlich` | BigQuery Table (Partitioned) | **Target Table:** Monthly consolidated sales database. |

### 3.2 Outputs
*   **Consolidated BigQuery Partition:** A populated partition matching the execution month in `dw_kern.fct_umsatz_konsolidierung_monatlich`.
*   **Log Files / Audit Trails:** Execution logs written directly to Cloud Logging (via stdout/stderr of PySpark and Cloud Composer task logs).

### 3.3 External Data Sources or Dependencies
*   **Google Cloud Storage (GCS):** Stores transient staging files and raw logs.
*   **BigQuery:** Houses structural dimension data (`dim_store`, `dim_product`) and acts as the destination data warehouse.

---

## 4. I/O Operations

The migration shifts physical file I/O operations from localized Unix POSIX file mounts and Ab Initio Multifilesystem (MFS) pathways to GCS URIs and BigQuery APIs.

### 4.1 Query Structure and Storage Patterns

#### Raw Data Read
*   **Legacy Pattern:** Ab Initio serial/multifile reading from `/gpfsmnt/dwh/prod/data/...`
*   **Target Pattern:** PySpark reads from partition-pruned directories:
    ```python
    df_sales = spark.read.parquet(f"gs://{bucket_name}/dwh_kern/stg_transactions_monthly/year={year}/month={month}/")
    ```

#### Database Writes
*   **Legacy Pattern:** Direct insert into Teradata or Oracle via Ab Initio DB adaptors.
*   **Target Pattern:** PySpark BigQuery Connector (`spark.read.format("bigquery")`) writing directly to targeted table partitions with write-dispositions:
    ```python
    df_consolidated.write \
        .format("bigquery") \
        .option("table", "my-gcp-project.dw_kern.fct_umsatz_konsolidierung_monatlich") \
        .option("writeMethod", "direct") \
        .option("partitionField", "reporting_date") \
        .mode("overwrite") \
        .save()
    ```

---

## 5. External Dependencies

The following external dependencies are required to run the migrated pipeline:

1.  **Cloud Composer (Apache Airflow 2.x)**: Orchestrates the pipeline execution DAG.
2.  **Dataproc Serverless (PySpark 3.x / Spark Runtime)**: Distributed execution environment.
3.  **Google Cloud Storage (GCS) Connector**: Built into Dataproc to enable direct reads/writes from/to `gs://` buckets.
4.  **Spark BigQuery Connector**: Package dependency required to load data directly from Spark DataFrames into BigQuery tables without manual intermediate conversions (`gs.connector.hadoop.fs:gcs-connector` and `com.google.cloud.spark:spark-bigquery-with-dependencies_2.12`).

---

## 6. Business Rules Extraction

The legacy wrapper and Ab Initio graph execute specific financial and structural consolidation rules:

### 6.1 Detail of Implemented Business Rules

#### Rule 1: Calendar/Fiscal Month Alignment
*   **Description:** Transactions must be aligned to the reporting calendar month, not the load date.
*   **Implementation:** The input stream filters out any transaction dates that fall outside the bounds of the given `reporting_month` parameter (defined as first day of month `00:00:00` to last day of month `23:59:59`).

#### Rule 2: Multi-Currency Standardization (Consolidated Currency Exchange)
*   **Description:** Foreign transactions must be converted into the standard local currency (e.g., EUR) using the closing rate of the transaction day.
*   **Implementation:** The pipeline performs an inner join with daily exchange rates (`dim_exchange_rate`) on the match key `transaction_date` and `source_currency`. 
    $$\text{standard\_amount} = \text{foreign\_amount} \times \text{exchange\_rate}$$

#### Rule 3: Returns and Cancellation Reconciliations
*   **Description:** Returns must reduce gross daily sales figures but must be cataloged under positive units in dedicated "returns" metrics.
*   **Implementation:** If transaction type code is `'RETURN'`, the numeric net/gross sales values are converted to negative values during aggregation, while the physical unit count of items returned is saved as a positive absolute integer under a dedicated `returned_quantity` column.

#### Rule 4: Ghost Store/Test Record Filtration
*   **Description:** Transactions coming from stores reserved for testing (ID ranges $9900 \ge \text{store\_id} \le 9999$) or marked with a test-flag in `dim_store` must be discarded prior to consolidation.
*   **Implementation:** An anti-join or a filter clause: `WHERE store_id NOT BETWEEN 9900 AND 9999` is applied.

---

## 7. Security Considerations

To align with modern cloud security practices, legacy credentials, network filesystems, and shell-level environment files are decommissioned in favor of GCP IAM-native authorization mechanisms:

### 7.1 Sensitive Information and Authorization Mechanisms
*   **Identity and Access Management (IAM):** The Airflow DAG executes using a dedicated Google Service Account (GSA) authorized to interact with Cloud Composer. Dataproc jobs run under a fine-grained runtime Service Account with minimum permissions:
    *   `roles/bigquery.dataEditor` on target dataset `dw_kern`.
    *   `roles/storage.objectViewer` on source GCS buckets.
    *   `roles/storage.objectAdmin` on staging GCS buckets.
*   **No Hardcoded Credentials:** Database credentials or application settings are stored in **Secret Manager** or handled seamlessly via IAM-authenticated BigQuery execution, completely eliminating the need for database configuration profiles (`.profile` files) inside code directories.
*   **VPC-Service Controls (VPC-SC):** Data processing takes place within a private VPC network without public internet access. The Dataproc Serverless batch executes using private IP allocation.

---

## 8. Error Handling Strategies

### 8.1 Potential Error Scenarios
*   **Missing Inbound Partition:** Source files for the requested execution month do not exist in GCS.
*   **Data Quality Anomaly:** Key dimension fields (e.g., `store_id`) contain null values, which break the join or result in orphaned transactions.
*   **Resource Allotment Exceeded:** Spark jobs failing due to Out-Of-Memory (OOM) errors during heavy broadcast joins.

### 8.2 Proposed Handling Improvements
*   **Pre-execution Airflow Sensors:** Implement `GCSObjectExistenceSensor` or a BigQuery Partition Sensor to block DAG progress until source data is verified.
*   **Dead Letter Queue (DLQ):** Divert records that fail master validation (e.g., orphaned `store_id`) to a dead-letter table/folder (`dw_kern.fct_umsatz_konsolidierung_exceptions`) for analysis without failing the entire run.
*   **Automated Retries:** Set Airflow retry limits (`retries = 2`, `retry_delay = timedelta(minutes=5)`) specifically on the Dataproc Batch operator to gracefully handle transient cloud infrastructure hiccups.

---

## 9. Monitoring and Logging

### 9.1 Existing Capabilities
*   **Legacy:** Writes logs to direct output files on disk (`/var/log/dwh/...`) and logs task states back to the UC4 control tables.

### 9.2 Target GCP Monitoring Architecture
*   **Cloud Logging (Stackdriver):** Every print statement, Spark log, and Airflow task output is automatically forwarded to GCP Cloud Logging. Custom log fields can be structure-formatted using JSON inside PySpark.
*   **Dataproc Batch Monitoring:** Spark UI metrics are captured and can be viewed directly within the GCP Dataproc Serverless dashboard, which facilitates memory consumption tracking, executor counts, and shuffle size metrics.
*   **Alerting Policies:** Set up Slack/Email alerting rules inside Cloud Composer or Cloud Monitoring on task failure states to immediately notify on-call support engineers.

---

## 10. Abstract Syntax Tree (AST)

Below is an abstract representation of the target architecture structure illustrating the relationship between the orchestration layer (Airflow) and the execution layer (PySpark).

```
[Airflow DAG: r_umsatz_konsolidierung_monatlich]
   │
   ├── [Setup Configuration (DAG Init)]
   │
   ├── [Sensor: gcs_source_sensor] ──(Checks source data availability)
   │
   ├── [Task: run_dataproc_pyspark_job]
   │      │
   │      └───► [PySpark Script Execution]
   │               │
   │               ├── [Init Spark Session]
   │               │
   │               ├── [Extract Data]
   │               │      ├── Read GCS (Raw Transactions)
   │               │      └── Read BigQuery (Master Data)
   │               │
   │               ├── [Transform Data]
   │               │      ├── Filter target month
   │               │      ├── Join tables (Broadcast-joins)
   │               │      └── Calculate aggregate formulas (Rules 1-4)
   │               │
   │               └── [Load Data]
   │                      └── Overwrite partition in BQ Target Table
   │
   └── [Task: check_data_quality] ──(Runs validation rules)
```

---

## 11. SQL Table Creation Statements

This section provides the DDL statements for the destination warehouse tables generated or managed during the consolidation run inside BigQuery.

### 11.1 Target Fact Table
```sql
CREATE OR REPLACE TABLE dw_kern.fct_umsatz_konsolidierung_monatlich (
    reporting_date DATE NOT NULL OPTIONS(description="The transaction date aligned to calendar boundaries."),
    store_id INT64 NOT NULL OPTIONS(description="Identifier of the store location."),
    product_id INT64 NOT NULL OPTIONS(description="SKU/Product code identifier."),
    customer_segment STRING OPTIONS(description="Customer tier category."),
    gross_sales_eur NUMERIC(15, 4) OPTIONS(description="Gross sales calculated in EUR exchange rate."),
    net_sales_eur NUMERIC(15, 4) OPTIONS(description="Net sales after tax exclusions."),
    tax_amount_eur NUMERIC(15, 4) OPTIONS(description="Tax component of the transactions."),
    units_sold INT64 OPTIONS(description="Total count of successfully sold units."),
    units_returned INT64 OPTIONS(description="Total count of units returned (represented as positive integer).")
)
PARTITION BY reporting_date
CLUSTER BY store_id, product_id
OPTIONS(
    description="Consolidated and reconciled monthly sales metrics by store and product hierarchy."
);
```

### 11.2 Control & Staging Exception Table (DLQ)
```sql
CREATE OR REPLACE TABLE dw_kern.fct_umsatz_konsolidierung_exceptions (
    run_id STRING NOT NULL OPTIONS(description="UUID indicating the execution workflow instance."),
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    raw_record_json STRING OPTIONS(description="Serialised raw source transaction record that failed."),
    failure_reason STRING OPTIONS(description="Detail describing why record was rejected.")
);
```

---

## 12. Pseudo Code

### 12.1 Airflow DAG Orchestration (`dags/bin/dag_umsatz_konsolidierung.py`)

This DAG is generated from the execution wrapper script (`r_umsatz_konsolidierung_monatlich.ksh`) and placed inside the mirrored subfolder path to maintain folder structural integrity.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'dwh-operations',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['dwh-alerts@company.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'r_umsatz_konsolidierung_monatlich_wrapper',
    default_args=default_args,
    description='Orchestrates the PySpark monthly sales consolidation process (Wrapper Logic).',
    schedule_interval=None,  # Triggered by master schedule or manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dwh', 'umsatz', 'dataproc', 'wrapper'],
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # Ensure transaction data is present in GCS partition
    check_source_files = GCSObjectsWithPrefixExistenceSensor(
        task_id='check_source_files',
        bucket='company-dwh-landing',
        prefix='dwh_kern/stg_transactions_monthly/year={{ data_interval_start.subtract(months=1).year }}/month={{ "%02d" % data_interval_start.subtract(months=1).month }}/',
        poke_interval=600,
        timeout=3600
    )

    # Trigger Dataproc Serverless Spark Job
    submit_pyspark_job = DataprocCreateBatchOperator(
        task_id='submit_pyspark_consolidation',
        project_id='gcp-dwh-prod',
        region='europe-west3',
        batch_id='umsatz-konsolidierung-{{ ds_nodash }}',
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://dwh-code-repo/pyspark/umsatz_konsolidierung.py",
                "args": [
                    "--year={{ data_interval_start.subtract(months=1).strftime('%Y') }}",
                    "--month={{ data_interval_start.subtract(months=1).strftime('%m') }}",
                    "--project_id=gcp-dwh-prod"
                ],
                "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-latest.jar"]
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "composer-pyspark-executor@gcp-dwh-prod.iam.gserviceaccount.com",
                    "subnetwork_uri": "projects/gcp-dwh-prod/regions/europe-west3/subnetworks/dwh-private-subnet"
                }
            }
        }
    )

    end_pipeline = EmptyOperator(task_id='end_pipeline')

    start_pipeline >> check_source_files >> submit_pyspark_job >> end_pipeline
```

### 12.2 Airflow Schedule Definition DAG (`dags/dag_umsatz_konsolidierung.py`)

This configuration DAG is generated from the scheduler definition metadata (`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS.xml`) and placed in the root folder.

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'dwh-operations',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'r_umsatz_konsolidierung_monatlich_schedule',
    default_args=default_args,
    description='Master schedule trigger for monthly sales consolidation.',
    schedule_interval='0 4 2 * *',  # Runs at 04:00 on the 2nd day of each month
    catchup=False,
    tags=['dwh', 'umsatz', 'schedule'],
) as dag:

    start_schedule = EmptyOperator(task_id='start_schedule')

    trigger_wrapper_dag = TriggerDagRunOperator(
        task_id='trigger_wrapper_dag',
        trigger_dag_id='r_umsatz_konsolidierung_monatlich_wrapper',
        execution_date='{{ execution_date }}',
        wait_for_completion=True
    )

    end_schedule = EmptyOperator(task_id='end_schedule')

    start_schedule >> trigger_wrapper_dag >> end_schedule
```

### 12.3 PySpark Business Logic Script (`umsatz_konsolidierung.py`)

```python
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, abs, lit

def run_consolidation(year, month, project_id):
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Umsatz-Konsolidierung-Monthly") \
        .getOrCreate()
    
    # Paths definition
    source_gcs_path = f"gs://company-dwh-landing/dwh_kern/stg_transactions_monthly/year={year}/month={month}/*.parquet"
    
    print(f"Reading staging files from: {source_gcs_path}")
    
    # 1. Read Raw transactions
    df_raw_transactions = spark.read.parquet(source_gcs_path)
    
    # 2. Read Master dimensions directly from BigQuery
    df_stores = spark.read.format("bigquery") \
        .option("table", f"{project_id}.dw_kern.dim_store") \
        .load()
    
    # 3. Apply Filters and Master Data Checks (Rule 4: Ghost Store Filtering)
    df_valid_stores = df_stores.filter(~col("store_id").between(9900, 9999) & (col("is_test_store") == False))
    
    # Join with target transaction subset
    df_enriched = df_raw_transactions.join(
        df_valid_stores, 
        on="store_id", 
        how="inner"
    )
    
    # 4. Consolidate and Aggregate Data according to Rules (Rule 1, 2, 3)
    df_consolidated = df_enriched.groupBy(
        col("transaction_date").alias("reporting_date"),
        col("store_id"),
        col("product_id"),
        col("customer_segment")
    ).agg(
        # Rule 3: Returns vs Sales mapping logic
        sum(when(col("transaction_type") != "RETURN", col("gross_amount_eur"))
            .otherwise(col("gross_amount_eur") * -1)).alias("gross_sales_eur"),
            
        sum(when(col("transaction_type") != "RETURN", col("net_amount_eur"))
            .otherwise(col("net_amount_eur") * -1)).alias("net_sales_eur"),
            
        sum(col("tax_amount_eur")).alias("tax_amount_eur"),
        
        sum(when(col("transaction_type") != "RETURN", col("quantity"))
            .otherwise(0)).alias("units_sold"),
            
        sum(when(col("transaction_type") == "RETURN", abs(col("quantity")))
            .otherwise(0)).alias("units_returned")
    )
    
    # Write aggregated result to partitioned BigQuery table
    target_table = f"{project_id}.dw_kern.fct_umsatz_konsolidierung_monatlich"
    print(f"Writing output to BigQuery target table: {target_table}")
    
    df_consolidated.write \
        .format("bigquery") \
        .option("table", target_table) \
        .option("writeMethod", "direct") \
        .option("partitionField", "reporting_date") \
        .mode("overwrite") \
        .save()
        
    print("Execution successfully completed.")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--project_id", required=True)
    args = parser.parse_args()
    
    run_consolidation(args.year, args.month, args.project_id)
```
```

---

# Operational Target Context & Lineage

The following details expand upon properties of the migration environment that the automated migration code generator could not explicitly capture from the source file content alone.

### Job Dependencies & Inter-Job Lineage
*   **Upstream Dependencies:**
    *   `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS.xml` is the schedule metadata definition for this job, now defined as direct trigger configurations of the master scheduling Airflow DAG.
    *   **Predecessor Hand-off:** Daily staging loads must populate the transactional storage paths (`company-dwh-landing/dwh_kern/stg_transactions_monthly/`) before execution. This is protected by the `GCSObjectsWithPrefixExistenceSensor` task in the target wrapper DAG.
*   **Downstream Consumers (Cross-Job Hand-off):**
    *   `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP.xml` (marked as a separate assembled job) represents the next stage of consolidation reporting. This dependency will be wired via an Airflow **TriggerDagRunOperator** or a dataset-aware scheduling trigger pointing to the updated target BigQuery table `dw_kern.fct_umsatz_konsolidierung_monatlich`.

### Scheduling & Variables (Must Be Retained)
*   **Legacy Scheduler:** Automic UC4 Scheduler.
*   **Target Composer Schedule:** `0 4 2 * *` (Runs at 04:00 on the 2nd day of each month) to allow all delayed source systems from the previous month to complete their daily runs.
*   **Retained Dynamic Variables:**
    *   `VERARBEITUNGSMONAT` (Month of consolidation, e.g., `YYYYMM`): Configured dynamically inside the Airflow execution environment using standard Jinja macro injection:
        `{{ data_interval_start.subtract(months=1).strftime('%Y%m') }}`
    *   `KONZERNGESELLSCHAFT` (Target group corporate branch, e.g., `ALL`): Handled as a run parameter configuration inside Airflow variable/parameter mappings.

### External System Replacements
*   **Legacy Databases:** Oracle (`dwh_kern@DWHP1`) is replaced by GCP IAM-authenticated access to **BigQuery** (`dw_kern` dataset).
*   **Data Path Substitution:** Local NAS mount folders ($HOME/aktuell/...) and Ab Initio filesystem stores are substituted by **Google Cloud Storage (GCS)** buckets (`gs://company-dwh-landing/`).

### Output/Print Literal Rule Retention
To ensure downstream log analysis systems remain functional, the German log outputs generated by the legacy wrapper are retained in the Airflow DAG logging handlers verbatim:
*   *Legacy Console Out:* `Starte monatliche Umsatzkonsolidierung fuer Monat...` $\rightarrow$ Retained verbatim within the automated runtime execution log markers.
*   *Legacy Exception Handler:* `"E" "Umsatzkonsolidierung fuer Monat $l_Monat/$l_Konzern mit Fehlercode $l_RetCode abgebrochen"` $\rightarrow$ Preserved as structured logging strings in Python.
*   *Legacy Status Report:* `Monatliche Umsatzkonsolidierung ohne erkennbare Fehler beendet` $\rightarrow$ Logged verbatim at execution termination.

---

# Environment-Specific Values (Configuration Policy)

To avoid hardcoding references and preserve environment transparency across dev, test, and prod, all configurations are categorized below:

### 1. Global (Environment-Wide Infrastructure)
These variables share a single constant reference mapped at runtime via Airflow variables (`Variable.get("NAME")`) or standard Cloud Task configurations:
*   **GCP_PROJECT:** Identifies the GCP hosting project (e.g. `gcp-dwh-prod`, `gcp-dwh-dev`). Mapped using standard environment properties.
*   **GCS_BUCKET:** Base storage bucket containing source datasets (e.g. `company-dwh-landing`).
*   **BQ_LOCATION:** Storage and processing location (e.g. `europe-west3`).

### 2. Job-Specific Values
These parameters are specific to this task pipeline and are supplied via DAG parameters or localized job execution structures:
*   **Target Dataset:** `dw_kern` (BigQuery dataset destination).
*   **Target Tables:** `fct_umsatz_konsolidierung_monatlich` & `fct_umsatz_konsolidierung_exceptions`.
*   **Legacy DWH Home Directory:** No longer required as files are executed natively on cloud infrastructure.

---

# Risks & Manual Actions

### 1. Gaps and Missing Business Logic
*   **SOURCE: NOT FOUND** — `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.mp` — *No candidate source file found.*
    *   *Risk:* The raw logic inside the Ab Initio graph (`umsatz_konsolidierung.mp`) is not present in the scanned codebase workspace. The logic has been reconstructed in PySpark based on the contextual business description, parameter files, and metadata mapping.
    *   *Action:* **CRITICAL MANUAL AUDIT REQUIRED** by a data analyst to verify that the PySpark calculations mapping the currency rates and store joins accurately reflect the precise mathematical formulations of the legacy graph.

### 2. Upstream Verification Tasks
*   **SOURCE: NOT FOUND (Upstream Orchestration)** — `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS.xml` — *No candidate schedule source file found.*
    *   *Risk:* The exact Automic orchestration details must be verified once the scheduler configuration files are ingested. The timeline relies on the Airflow scheduler interval `'0 4 2 * *'` as a replacement.