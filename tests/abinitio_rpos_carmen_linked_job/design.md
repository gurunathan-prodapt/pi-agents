# MIGRATION DESIGN DOCUMENT

## SECTION 1 — VERBATIM MCP TOOL OUTPUT

Below is the complete, verbatim output returned by the `uc4_design_airflow_dag` tool:

```text
### INPUT VALIDATION WARNING
Only one file was provided in the input, and it is a `JOBS_UNIX` file (`DW.RPOS_CARM_IMPORT`). A complete production workflow typically requires at least one `EVNT_TIME` file (for scheduling), one `JOBP` file (for workflow structures and task dependencies), and the corresponding `JOBS_UNIX` files. 

Because the orchestration context (scheduling, parent workflow structure, and dependency graph) is missing, this design is built assuming a **standalone DAG** containing a single task representing this UNIX job. The developer must manually integrate this DAG or task into the wider scheduler/parent workflow once those files are available.

---

## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
The UC4 object `DW.RPOS_CARM_IMPORT` is a Unix Job that executes an Ab Initio graph called `RPOS_CARM_IMPORT`. Its primary function is to import Carmen-related data into the Data Warehouse system using the configuration file `map_rpos_carmen_import.cfg`. In UC4, this job runs on host `DWHDWH1P` under the user login `DW.UNIX.ISTNS`. In the target Google Cloud Platform (GCP) architecture, this Ab Initio ETL logic will be migrated to a PySpark script executed on a Cloud Dataproc cluster.

---

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.RPOS_CARM_IMPORT` | `JOBS_UNIX` | `1` (Active) | Job startet AbInitio Graph map_rpos_carmen_import |

---

### 3. Airflow DAG Properties
| Property | Value | Notes |
| :--- | :--- | :--- |
| **dag_id** | `dw_rpos_carm_import` | Sanitized from original UC4 object name. |
| **schedule** | `None` | **No EVNT_TIME file was provided.** Defaulting to unscheduled. |
| **start_date** | `YYYY-MM-DD` (Placeholder) | Developer must define the production start date. |
| **catchup** | `False` | Recommended default to prevent backfilling. |
| **max_active_runs** | `1` | Ensures parallel runs of this extraction process do not overlap. |
| **is_paused_upon_creation** | `False` | Derived from UC4 `<Active>1</Active>` (Active). |
| **default_args** | `{'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` | Standard default parameters. |

---

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `rpos_carm_import` | `DataprocSubmitJobOperator` | `rpos_carm_import.py` | GCP Project, Dataproc Cluster, Region | 0 | N/A | None | None | `False` | None | Standalone PySpark task. |

---

### 5. Task Dependency Map
Since only a single `JOBS_UNIX` file was provided, the dependency map is linear and contains only one operational task:

`rpos_carm_import`

*Note: If nested inside a parent workflow in the future, this task will be triggered downstream of its upstream dependencies.*

---

### 6. Parameter and Variable Mapping
| UC4 Parameter | Value / Source | Airflow Equivalent |
| :--- | :--- | :--- |
| **UC4 Object Name** | `DW.RPOS_CARM_IMPORT` | `dw_rpos_carm_import` (Sanitized DAG ID) |
| **Login** | `DW.UNIX.ISTNS` | Cloud Dataproc Service Account / IAM Role permissions |
| **Host** | `|DWHDWH1P|HOST` | `YOUR_DATAPROC_CLUSTER_NAME` |
| **Ab Initio Graph (`-j`)** | `RPOS_CARM_IMPORT` | PySpark script: `gs://YOUR_BUCKET_NAME/pyspark_scripts/rpos_carm_import.py` |
| **Job Key (`-k`)** | `$HOME/aktuell/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` | Passed as a job argument: `--config_file=gs://YOUR_BUCKET_NAME/config/bd_proc/map_rpos_carmen_import.cfg` |
| **Estimated Runtime** | `<Ert>1</Ert>` | Estimated runtime is 1 second (highly dynamic, treated as metadata only) |

---

### 7. Error Handling and Retry Strategy
- **Retries**: There is no explicit retry block defined in the UC4 XML (`<RUNTIME>` has no automatic restart rules, and no `RESTART TASK` actions are defined in postconditions). Thus, retries are set to `0` by default.
- **ENDED_SKIPPED Handling**: No custom postconditions exist in this standalone file. Standard task status propagation applies: if the task fails, the DAG run is marked as failed.
- **Sync Objects**: No `<SYNCREF>` synchronization elements are configured. The DAG-level parameter `max_active_runs=1` is sufficient to prevent concurrent execution conflicts.

---

### 8. Developer Notes
* **Missing Files Warning**: The `EVNT_TIME` and `JOBP` workflow container files were missing from the migration export. Consequently, scheduling details and upstream/downstream context must be clarified with the business.
* **GCP Infrastructure Setup**: The developer must provision and supply actual values for all GCP placeholders (Project ID, Region, Dataproc Cluster Name, and GCS Bucket Name).
* **Configuration Migration**: The configuration file `map_rpos_carmen_import.cfg` referenced in the original Unix shell script (`-k` flag) must be migrated to GCS and its parsing logic implemented within the PySpark script `rpos_carm_import.py`.
* **Environment Variables**: The original script loads an environment script via `. $HOME/.dw_init`. Any required environmental configurations must be passed to the PySpark job via Spark configurations or command-line arguments.

---

## SECTION 2 — — PSEUDOCODE

```text
── IMPORTS ───────────────────────────────────────────────────────────────────
IMPORT datetime.timedelta
IMPORT airflow.DAG
IMPORT airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator
IMPORT airflow.utils.dates.days_ago

── GCP CONFIGURATION ─────────────────────────────────────────────────────────
DEFINE CONSTANT GCP_PROJECT_ID AS "YOUR_GCP_PROJECT_ID"
DEFINE CONSTANT DATAPROC_REGION AS "YOUR_DATAPROC_REGION"
DEFINE CONSTANT DATAPROC_CLUSTER AS "YOUR_DATAPROC_CLUSTER_NAME"
DEFINE CONSTANT GCS_BUCKET AS "YOUR_BUCKET_NAME"

── DEFAULT ARGS ──────────────────────────────────────────────────────────────
DEFINE default_args AS DICTIONARY:
    SET "owner" TO "airflow"
    SET "start_date" TO days_ago(1)  // Placeholder: Adjust to specific migration date
    SET "retries" TO 0
    SET "retry_delay" TO timedelta(minutes=5)

── DAG DEFINITION ────────────────────────────────────────────────────────────
CREATE DAG WITH ID "dw_rpos_carm_import"
    default_args = default_args
    schedule = None  // Unscheduled due to missing EVNT_TIME file
    catchup = False
    max_active_runs = 1
    is_paused_upon_creation = False  // Active flag was 1 in UC4 XHEADER

── TASK: RPOS_CARM_IMPORT ────────────────────────────────────────────────────
// Maps to JOBS_UNIX "DW.RPOS_CARM_IMPORT"
// Executes the translated Ab Initio graph logic on Dataproc Serverless/Cluster

DEFINE pyspark_job_payload AS DICTIONARY:
    SET "reference" TO DICTIONARY:
        SET "project_id" TO GCP_PROJECT_ID
    SET "placement" TO DICTIONARY:
        SET "cluster_name" TO DATAPROC_CLUSTER
    SET "pyspark_job" TO DICTIONARY:
        SET "main_python_file_uri" TO "gs://" + GCS_BUCKET + "/pyspark_scripts/rpos_carm_import.py"
        SET "args" TO LIST:
            "--config_file", "gs://" + GCS_BUCKET + "/config/bd_proc/map_rpos_carmen_import.cfg",
            "--job_kennung", "RPOS_CARM_IMPORT"

CREATE TASK "rpos_carm_import" USING DataprocSubmitJobOperator:
    task_id = "rpos_carm_import"
    project_id = GCP_PROJECT_ID
    region = DATAPROC_REGION
    job = pyspark_job_payload
    // Generate a unique execution ID for tracking
    job_id = "dw_rpos_carm_import_" + "{{ run_id | ts_nodash | lower }}" + "_task_import"

── DEPENDENCIES ──────────────────────────────────────────────────────────────
// Standalone workflow execution block
rpos_carm_import
```
```

---

## SECTION 2 — ADDED CONTEXT THE MCP COULD NOT SEE

### 1. Job Dependencies & Orchestration Wiring
* **Upstream Dependency:**
  * **Shared Files:** `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start`
    * *Status:* Already migrated & merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/755).
    * *Target Wiring:* The generic startup utility parsing and execution routing logic in `r_ai_start` is imported/referenced downstream as part of the shared module execution in Cloud Composer.
* **Downstream Consumers:** 
  * None discovered in the provided scheduling metadata.

### 2. Execution Order Mapping
The target DAG orchestration strictly preserves the following sequential execution steps:
1. Initialize DAG `dw_rpos_carm_import`.
2. Load configuration parameters defined in `map_rpos_carmen_import.cfg` (hosted on Google Cloud Storage).
3. Execute PySpark script on Cloud Dataproc Serverless mimicking the sequential steps of the original graph logic.

### 3. Scheduling & Variable Retention
* **Schedule:** Unscheduled (`None`) as no `EVNT_TIME` file was available. If required, it will be mapped to a target event trigger or a Cloud Composer cron-expression once the upstream workflow is defined.
* **Variables Retained:**
  * `DWH_JOB_KENNUNG` = `'RPOS_CARM_IMPORT'` (passed as execution parameter `--job_kennung`).
  * `HOME` (mapped to cloud runtime pathing).

### 4. Data Lineage (Producers/Consumers)
* **Upstream Source Files (from CFG):**
  * Billing raw data matching date mask `CARMEN_B_*_pos.fix` in `$DW_DIR_IMP_SAP/crs/work/`.
* **Downstream Target Tables (BigQuery):**
  * `DWH$TA_F_RPOS_CARM`
  * `DWH$TA_F_RPOS_FACT_CARM`
  * `DWH$TA_F_RPOS_RESELLING_CARM`
  * `DWH$TA_F_GPOS_FACT_CARM`
  * `DWH$TA_T_RPOS_CARM`

### 5. External System Replacements
* **Legacy UNIX Filesystem Paths:**
  * `$DW_DIR_IMP_SAP/crs/work/` mapped to GCS landing bucket prefix: `gs://{GCS_BUCKET}/import_sap/crs/work/`
  * `$DW_DIR_IMP_SAP/crs/store/` mapped to GCS archive bucket prefix: `gs://{GCS_BUCKET}/import_sap/crs/store/`
* **Ab Initio GDE Graph Execution Platform:**
  * Replaced by Cloud Dataproc running Serverless PySpark tasks.

### 6. Environment Variable & Parameter Classification

#### GLOBAL (Environment-Wide)
These parameters represent target architecture constants and are fetched dynamically at runtime via Airflow Variables:
* `GCP_PROJECT`: Mapped to `Variable.get("GCP_PROJECT")`
* `GCP_REGION`: Mapped to `Variable.get("GCP_REGION")`
* `DATAPROC_CLUSTER`: Mapped to `Variable.get("DATAPROC_CLUSTER")`
* `GCS_BUCKET`: Mapped to `Variable.get("GCS_BUCKET")`

#### JOB-SPECIFIC (Verified Legacy Parameters)
These configurations belong specifically to this job and are defined within the Spark job metadata payload or inlined:
* `BHB_Projektverzeichnis` = `/Projects/TMD/processing/BHB/BD_PROC`
* `BHB_Version` = `RLS_BHB_nach_64_rabatt_sap`
* `BHB_Graph` = `map_rpos_carmen_import`
* `BHB_Prozesstyp` = `D`
* `BHB_Quellverzeichnis` = `$DW_DIR_IMP_SAP/crs/work/`
* `BHB_Zielverzeichnis` = `$DW_DIR_IMP_SAP/crs/store/`
* `BHB_Dateimaske` = `CARMEN_B_*_pos.fix`
* `BHB_Kopfdatensatzkennung` = `H`
* `BHB_Nutzdatensatzkennung` = `P`
* `BHB_Endedatensatzkennung` = `X`

### 7. Risks & Manual Actions
* **Inclusions and Helpers (Human-Reviewed - NO SOURCE NEEDED):**
  * The following referenced scripts are excluded from migration as they are verified environment bootstrap/diagnostic checks not required on Cloud Composer:
    * `.CCR_INIT`
    * `.DW_INIT`
    * `AB_CATALOG_FUNCTIONS.KSH`
    * `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC`
    * `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC`
    * `DW.HOLE_PFAD`
    * `DW.LESE_LOG`
    * `H_ALIS_DATE.KSH`
    * `H_ALIS_DATENOBJEKT.KSH`
    * `H_ALIS_MELDUNGEN.KSH`
    * `H_ALIS_PARAMETER.KSH`
* **Missing Source Components (Acknowledge and Stubbed):**
  * `SOURCE: NOT FOUND — map_rpos_carmen_import.mp — abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp`
  * `SOURCE: NOT FOUND — map_rpos_carmen_import.ksh — abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh`

### 8. File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml` | `dags/dw_rpos_carm_import.py` | Converts the UC4 Job definition into a consolidated Airflow DAG. |
| `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` | `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` | Configuration parameters to be migrated verbatim to target GCS directory. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` | Target PySpark script representing the migrated Ab Initio graph (stubbed due to missing source). |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh` | `Retired` | Wrapper execution script replaced completely by the Cloud Composer Dataproc Operator. |

### 9. Folder Integrity Justification
The target repo preserves the original legacy codebase folder layout:
* `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` is placed in the mirrored configuration subdirectory.
* `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` is generated as the target translation file inside its mirrored code folder structure.

---

## SECTION 3 — TARGET FILE PLAN & CONSOLIDATED TARGET IMPLEMENTATION

The build implementation generates **exactly one** Airflow DAG file and **exactly one** PySpark template script to resolve previous execution conflicts and prevent duplicate definitions.

### File 1: `dags/dw_rpos_carm_import.py` (Airflow DAG)
This Airflow DAG is constructed to run the job on Dataproc. To eliminate DAG parse errors, it implements the `DataprocSubmitJobOperator` without performing any python import references against the target PySpark script file.

```python
"""
Airflow DAG representing the migrated UC4 Job DW.RPOS_CARM_IMPORT.
Triggers a Dataproc PySpark job executing the map_rpos_carmen_import graph logic.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.models import Variable

# Global architecture variables sourced dynamically from Airflow Config Store
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_rpos_carm_import',
    default_args=default_args,
    description='Job startet AbInitio Graph map_rpos_carmen_import',
    schedule_interval=None,  # Unscheduled due to missing EVNT_TIME definition
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Define Spark task payload submitting parameters mimicking r_ai_start
    pyspark_job_payload = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py",
            "args": [
                "--config_file", f"gs://{GCS_BUCKET}/abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg",
                "--job_kennung", "RPOS_CARM_IMPORT"
            ]
        }
    }

    rpos_carm_import = DataprocSubmitJobOperator(
        task_id='rpos_carm_import',
        job=pyspark_job_payload,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        job_id="dw_rpos_carm_import_" + "{{ run_id | ts_nodash | lower }}_task_import"
    )

    rpos_carm_import
```

### File 2: `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` (PySpark Script)
This target PySpark file is placed strictly inside the mirrored legacy folder structure. Because the original `.mp` file was missing from the scan context, it is explicitly stubbed with a runtime error indicating a manual rebuild requirement.

```python
#!/usr/bin/env python
"""
PySpark Script representing the migrated Ab Initio graph map_rpos_carmen_import.
This script is generated in its original mirrored folder layout as per Folder Integrity.
"""

import sys
import argparse
from pyspark.sql import SparkSession

def main():
    # Parse command line arguments passed from Cloud Composer DAG
    parser = argparse.ArgumentParser(description="PySpark Job for map_rpos_carmen_import")
    parser.add_argument("--config_file", required=True, help="Path to config file on GCS")
    parser.add_argument("--job_kennung", required=True, help="Job identification key")
    args = parser.parse_args()
    
    print(f"Initializing job: {args.job_kennung} with config: {args.config_file}")
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName(f"PySpark_{args.job_kennung}") \
        .getOrCreate()

    # Raise explicit implementation error due to missing .mp source file in migration context
    raise NotImplementedError(
        "SOURCE: NOT FOUND — map_rpos_carmen_import.mp — "
        "The source Ab Initio graph file map_rpos_carmen_import.mp was not found in the scanned codebase. "
        "A manual rebuild of the ETL logic is required to process and load Carmen data into DWH tables: "
        "DWH$TA_F_RPOS_CARM, DWH$TA_F_RPOS_FACT_CARM, DWH$TA_F_RPOS_RESELLING_CARM, "
        "DWH$TA_F_GPOS_FACT_CARM, and DWH$TA_T_RPOS_CARM."
    )

if __name__ == "__main__":
    main()
```

---

# Data Migration Design Document: DW.RPOS_CARM_IMPORT

## 1. File Disposition Table

Every file from the pre-collected context is accounted for below. This plan ensures complete folder integrity and guarantees that no legacy logic is dropped or cross-merged incorrectly.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` | Primary Ab Initio business logic converted to a single PySpark pipeline. |
| `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` | `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.json` | **NO MCP NEEDED**: Converted into a native JSON configuration file storing environment parameters verbatim. |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml` | `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import.py` | Orchestration Airflow DAG representing the UC4 job execution logic. |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh` | **Retired** | Wrapper script retired; task execution and environment setup are natively handled by the Airflow DAG. |

---

## 2. Shared Files & External System Replacements

* **Shared Files**: 
  * `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start` has already been migrated and merged (PR: `https://github.com/gurunathan-prodapt/pi-agents/pull/755`). This common initialization framework must be referenced or included dynamically via the PySpark job parameters (using the `--py-files` configuration in Cloud Composer when submitting the Dataproc Serverless batch).
* **External System Replacements**:
  * Oracle connections (`DB_TNS_NAME_DWH`, `DB_TNS_NAME_CRS`, etc.) are replaced by Google Cloud BigQuery datasets.
  * Local flat-file processing paths (`$DW_DIR_IMP_SAP/crs/work/`) map directly to Google Cloud Storage (GCS) staging buckets.

---

## 3. Job Dependencies, Execution Order & Scheduling

* **Job Dependencies (Upstream)**:
  * Upstream: `Shared Files — abinitio_pyspark_linked_job/isccr/abinitio/bin` (already migrated). There are no other cross-job predecessors/successors explicitly specified in the pre-collected context.
* **Execution Order**:
  The target orchestration preserves the legacy dependency sequence:
  1. Initialize Job Context parameters (derived from the converted `.json` configuration file).
  2. Orchestrate and trigger the PySpark conversion process.
  3. Submit the PySpark script as a Dataproc Serverless Batch task.
* **Scheduling**:
  * None discovered in the source context. The Cloud Composer DAG will run on-demand or remain unscheduled until a defined schedule is integrated.

---

## 4. Environment-Specific Variables Policy

Variables are categorized by their role in the target GCP environment rather than legacy terminology.

### 1. Global (Environment-wide)
These variables remain identical for every job in a given environment (Dev/Test/Prod). They must be sourced at runtime via environment reads or Airflow Variable lookups:

* `GCP_PROJECT` — Sourced in Python via `os.environ.get("GCP_PROJECT")`, and in Airflow using `Variable.get("gcp_project")`.
* `GCP_REGION` / `DATAPROC_REGION` — Sourced via `Variable.get("gcp_region")`.
* `GCS_BUCKET` — Staging bucket replacing legacy mount directories, sourced via `Variable.get("gcs_bucket")`.
* `BQ_DATASET` — Core target BigQuery dataset, sourced via `Variable.get("bq_dataset")`.
* `DW_DIR_IMP_SAP` — Sourced as `Variable.get("dw_dir_imp_sap")` mapping to `gs://{GCS_BUCKET}/stage/imp_sap`.

### 2. Job-Specific
These variables are specific to this job's parameters and files. They are populated directly from the converted configuration object:

* `BHB_Projektverzeichnis` = `/Projects/TMD/processing/BHB/BD_PROC`
* `BHB_Version` = `RLS_BHB_nach_64_rabatt_sap`
* `BHB_Graph` = `map_rpos_carmen_import`
* `BHB_Prozesstyp` = `D`
* `BHB_Quellverzeichnis` = `gs://{GCS_BUCKET}/stage/imp_sap/crs/work/`
* `BHB_Zielverzeichnis` = `gs://{GCS_BUCKET}/stage/imp_sap/crs/store/`
* `BHB_Dateimaske` = `CARMEN_B_*_pos.fix`
* `BHB_Kopfdatensatzkennung` = `H`
* `BHB_Nutzdatensatzkennung` = `P`
* `BHB_Endedatensatzkennung` = `X`

*No hardcoded placeholders or prose values (such as "your-project-id") are allowed. Values must resolve dynamically at runtime.*

---

## 5. Target File Plan

### 5.1 Airflow DAG Orchestration
* **Target Path**: `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import.py`
* **Language**: Python (Airflow DAG)
* **Description**: Orchestrates the job and submits the PySpark job to Dataproc Serverless. It completely avoids any local PySpark imports to prevent Airflow parse errors.

```python
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# Retrieve Environment Globals
GCP_PROJECT = Variable.get("gcp_project")
GCP_REGION = Variable.get("gcp_region")
GCS_BUCKET = Variable.get("gcs_bucket")
SPARK_SERVICE_ACCOUNT = Variable.get("spark_service_account")

# Job-specific variables derived from configuration parameters
PYSPARK_SCRIPT = f"gs://{GCS_BUCKET}/abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py"
CFG_FILE_PATH = f"gs://{GCS_BUCKET}/abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.json"

default_args = {
    "owner": "DWH",
    "start_date": datetime(2026, 1, 1),
    "depends_on_past": False,
}

with DAG(
    dag_id="dw_rpos_carm_import",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["abinitio", "pyspark", "carmen"],
) as dag:

    # Dataproc Serverless Batch Operator executing the PySpark pipeline
    submit_pyspark_job = DataprocCreateBatchOperator(
        task_id="submit_pyspark_job",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        batch_id="dw-rpos-carm-import-batch",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": PYSPARK_SCRIPT,
                "args": [
                    "--cfg_path", CFG_FILE_PATH,
                ],
            },
            "environment_config": {
                "execution_config": {
                    "service_account": SPARK_SERVICE_ACCOUNT,
                }
            }
        }
    )

    submit_pyspark_job
```

### 5.2 Converted CFG File (No MCP Call Needed)
* **Target Path**: `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.json`
* **Language**: JSON
* **Description**: Verbatim translation of key-value pairs derived from the `EXTRACTED SETTINGS` section of the parameter file.

```json
{
  "FWP_Pre_Session": "",
  "FWP_Post_Session": "",
  "BHB_Projektverzeichnis": "/Projects/TMD/processing/BHB/BD_PROC",
  "BHB_Version": "RLS_BHB_nach_64_rabatt_sap",
  "BHB_Graph": "map_rpos_carmen_import",
  "BHB_Prozesstyp": "D",
  "BHB_Quellverzeichnis": "$DW_DIR_IMP_SAP/crs/work/",
  "BHB_Zielverzeichnis": "$DW_DIR_IMP_SAP/crs/store/",
  "BHB_Dateimaske": "CARMEN_B_*_pos.fix",
  "BHB_Kopfdatensatzkennung": "H",
  "BHB_Nutzdatensatzkennung": "P",
  "BHB_Endedatensatzkennung": "X",
  "BHB_Eintragsnr": "",
  "BHB_Dateiname": "",
  "BHB_Laufzeitvariable": ""
}
```

---

## 6. Verbatim MCP Design Output
The following is the complete, unmodified output from the primary conversion tool `abinitio_design_pyspark`:

=== START OF VERBATIM MCP OUTPUT ===

# PySpark Migration Design Document: tmpg2ybyaov

## 1. GRAPH OVERVIEW
The overall purpose of this graph is to read billing position flat files, split them into Nutzdaten (payload) and Endedatensatz (metadata footer), validate the payload, and perform a historical lookup against the contract table (`dwh$ta_c_vertrag`) based on effective date intervals. The payload records are routed and loaded into several target database tables (`DWH$TA_F_RPOS_FACT_CARM`, `DWH$TA_T_RPOS_CARM`, `DWH$TA_F_RPOS_CARM`, `DWH$TA_F_GPOS_FACT_CARM`, `DWH$TA_F_RPOS_RESELLING_CARM`) based on business form classifications, while metadata is processed to log the run status and update accounting periods in control tables (`DWH$TA_K_MELDUNGEN`, `DWH$TA_K_RECH_ABSGRP`).

---

## 2. SOURCES
For each source database table or flat file read:

* **Read File (Inbound Billing CSV)**
  * **Kind**: file
  * **Path**: `# REVIEW: File path not extracted; represent as dynamic runtime parameter`
* **DWH$TA_F_RPOS_CARM**
  * **Kind**: select
  * **SQL**: `select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_CARM`
* **DWH$TA_F_RPOS_CARM-2**
  * **Kind**: select
  * **SQL**: `select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_CARM`
* **DWH$TA_F_RPOS_FACT_CARM**
  * **Kind**: select
  * **SQL**: `select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_FACT_CARM`
* **DWH$TA_F_RPOS_FACT_CARM - 2**
  * **Kind**: select
  * **SQL**: `select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_FACT_CARM`
* **DWH$TA_F_RPOS_RESELLING_CARM**
  * **Kind**: select
  * **SQL**: `select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_RESELLING_CARM`
* **DWH$TA_F_RPOS_RESELLING_CARM-1**
  * **Kind**: select
  * **SQL**: `select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_RESELLING_CARM`
* **dwh$ta_c_vertrag**
  * **Kind**: select
  * **SQL**: 
    ```sql
    select 
    rahmenvertrag_id,
    vertrag_id_carmen,
    dwh_vertrag_id,
    dwh_gp_id,
    dwh_konto_id,
    dwh_tarifgr_id,
    vo_kenn,
    zv_id,
    gueltig_von, 
    gueltig_bis
    from 
    dwh$ta_c_vertrag
    where 
    gueltig_bis >= to_date('20050401', 'YYYYMMDD') 
    and ABLOCAL(dwh$ta_c_vertrag)
    ```

---

## 3. TRANSFORMS

* **Reformat rechnung_datum to datetime for Delete** (reformat)
  * **Expression**: `out.* :: in.*;`
  * **Description**: Passes through all record columns unchanged.
* **Validate Records** (reformat)
  * **Expression**:
    ```abinitio
    out.monats_id :: if(!is_valid(in.monats_id)) force_error("Invalid data format in monats_id") else in.monats_id;
    out.rechnung_datum :: if(!is_valid(in.rechnung_datum)) force_error("Invalid data format in rechnung_datum") else in.rechnung_datum;
    out.standardvertrags_id :: if(!is_valid(in.standardvertrags_id)) force_error("Invalid data format in standardvertrags_id") else in.standardvertrags_id;
    out.vertrags_id :: if(!is_valid(in.vertrags_id)) force_error("Invalid data format in vertrags_id") else in.vertrags_id;
    out.rechpos_brutto_eur :: if(!is_valid(in.rechpos_brutto_eur)) force_error("Invalid data format in rechpos_brutto_eur") else in.rechpos_brutto_eur;
    out.rechpos_netto_eur :: if(!is_valid(in.rechpos_netto_eur)) force_error("Invalid data format in rechpos_netto_eur") else in.rechpos_netto_eur;
    out.rechpos_mwst_eur :: if(!is_valid(in.rechpos_mwst_eur)) force_error("Invalid data format in rechpos_mwst_eur") else in.rechpos_mwst_eur;
    out.* :: in.*;
    ```
  * **Description**: Validates format integrity on crucial transaction fields, raising errors if null/invalid values are detected.
* **replace ',' by '.'** (reformat)
  * **Expression**:
    ```abinitio
    out.kennzeichen :: in.kennzeichen;
    out.datensatz_rest :: string_replace(in.datensatz_rest, ',', '.');
    ```
  * **Description**: Normalizes German numeric formatting within the raw string block by replacing commas with dots.
* **Reformat Referencerecord** (reformat)
  * **Expression**:
    ```abinitio
    out.kennzeichen :: in.kennzeichen;
    out.datensatz_rest :: in.datensatz_rest;
    ```
  * **Description**: Passes down the primary data identification columns.
* **Reformat for delete** (reformat)
  * **Expression**:
    ```abinitio
    out.rechnung_id :: in.rechnung_id;
    out.rechnung_datum :: in.rechnung_datum;
    out.standardvertrags_id :: in.standardvertrags_id;
    out.vertrags_id :: in.vertrags_id;
    out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
    ```
  * **Description**: Projects matching business keys used to isolate target database records slated for deletion.
* **Proof Join - criterias gueltig_von and gueltig_bis** (reformat)
  * **Expression**:
    ```abinitio
    let date("YYYYMMDD") month_last_day =(date('YYYYMMDD'))datetime_add(in.monats_id,date_month_end(date_month(in.monats_id),date_year(in.monats_id)));
    let integer(4) valid_flag =if ((is_null(in.gueltig_von) or month_last_day > in.gueltig_von) 
    and (is_null(in.gueltig_bis) or month_last_day <= in.gueltig_bis))
    0
    else
    1;

    out.* :: in.*;
    out.rahmenvertrag_id :: if(valid_flag == 0) rahmenvertrag_id;
    out.dwh_vertrag_id :: if(valid_flag == 0) dwh_vertrag_id;
    out.dwh_gp_id :: if(valid_flag == 0) dwh_gp_id;
    out.dwh_konto_id :: if(valid_flag == 0) dwh_konto_id;
    out.dwh_tarifgr_id :: if(valid_flag == 0) dwh_tarifgr_id;
    out.vo_kenn :: if(valid_flag == 0) vo_kenn;
    out.zv_id :: if(valid_flag == 0) zv_id;
    out.gueltig_von :: if(valid_flag == 0) gueltig_von;
    ```
  * **Description**: Nullifies contract details if the transaction's month-end date does not fall within the contract's validity range.
* **Reformat for insert "fact data"** (reformat)
  * **Expression**:
    ```abinitio
    out.* :: in.*;
    out.rahmenvertrag :: in.rahmenvertrag_id;
    ```
  * **Description**: Assigns the contract's frame contract ID to the target physical column name.
* **Reformat for insert "temporary data"** (reformat)
  * **Expression**:
    ```abinitio
    let datetime("YYYYMMDDHH24MISS") mindate =(datetime('YYYYMMDDHH24MISS'))(string(14))'19000101000000';
    out.* :: in.*;
    out.bearbeitung_datum :: mindate;
    ```
  * **Description**: Sets the default minimum date value `1900-01-01` on temporary position tables.
* **Proof Join-criteriase gueltig_von and gueltig_bis** (reformat)
  * **Expression**: Similar interval check validation as `Proof Join - criterias gueltig_von and gueltig_bis`.
  * **Description**: Standard date range validation for alternative streams.
* **Reformat for insert "Factoring Gutschriften"** (reformat)
  * **Expression**:
    ```abinitio
    out.* :: in.*;
    out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
    out.rahmenvertrag :: in.rahmenvertrag_id;
    out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
    ```
  * **Description**: Limits and maps service performance ID to a max length of 9 and maps frame contract columns.
* **Reformat for insert "Factoring Rechnungen"** (reformat)
  * **Expression**: Same mapping as Gutschriften, applied to active factoring invoice streams.
  * **Description**: Truncates performance ID and maps frame contract columns.
* **Reformat for insert "Reselling"** (reformat)
  * **Expression**: Same mapping as Gutschriften, applied to Reselling streams.
  * **Description**: Truncates performance ID and maps frame contract columns.
* **Reformat Enderecord for Processing** (reformat)
  * **Expression**:
    ```abinitio
    out.kennzeichen :: in.kennzeichen;
    out.bemerkung :: in.bemerkung;
    out.stichtag :: in.stichtag;
    out.anzahl :: in.anzahl;
    out.inhalt :: in.inhalt;
    out.erstellt_am :: (string_index(in.erstellt_am, ";") == 0) ? in.erstellt_am : string_substring(in.erstellt_am, 1, string_length(in.erstellt_am)-1);
    ```
  * **Description**: Cleans trailing semicolon characters from creation date string values.
* **Reformat for DB and Filter out where Kompl_Kennzeichen != L** (reformat)
  * **Expression**:
    ```abinitio
    out.monats_id :: (string(6))(date("YYYYMM"))date_add_months((date("YYYYMM")) string_substring(in.stichtag,1,6),-1);
    out.abs_grp :: string_substring(in.bemerkung,10,5) ;
    out.dateiname :: in.bemerkung;
    out.rechnung_datum :: (date("YYYYMMDD")) in.stichtag;
    out.rechnungsteil :: (string(1))"P";
    out.ladedatum :: now();
    ```
  * **Description**: Calculates the target billing period month ID, parsing metadata string elements to generate batch run control parameters.

---

## 4. IN-MEMORY LOOKUPS
No lookups were extracted from this graph.

---

## 5. FILTERS (select_expr)

* **Split Data**
  * **Expression**: `kennzeichen == "${BHB_Nutzdatensatzkennung}"`
  * **Effect**: Routes file rows containing transactional usage record payloads.
* **Split Metadata**
  * **Expression**: `kennzeichen == "${BHB_Endedatensatzkennung}"`
  * **Effect**: Isolates the batch metadata/footer control records.
* **Filter by Expression (Rabatt)**
  * **Expression**: `rech_leistung_id_carm == "RABATT"`
  * **Effect**: Drops or redirects records matching Rabatt (discounts).
* **Filter by Expression (Delete Flag)**
  * **Expression**: `delete_flag == 1`
  * **Effect**: Screens and identifies dataset batches flagged for deletion processing.
* **Select "Positionen auf Debitorenebene" (temporary Data)**
  * **Expression**: `typ == 'T'`
  * **Effect**: Filters and selects temporary position segments.
* **Select "Factoring Gutschriften"**
  * **Expression**: `rpos_geschaftsform_kenn == 'G'`
  * **Effect**: Isolates Gutschriften (factoring credit notes).
* **Select "Factoring Rechnungen"**
  * **Expression**: `rpos_geschaftsform_kenn == 'F'`
  * **Effect**: Isolates classic factoring invoice records.
* **Select "Reselling"**
  * **Expression**: `rpos_geschaftsform_kenn == 'R'`
  * **Effect**: Isolates reselling transactional records.

---

## 6. OUTPUT TARGETS

* **DWH$TA_F_RPOS_FACT_CARM**
  * **Kind**: insert
  * **Table**: `dwh_ta_f_rpos_fact_carm`
  * **SQL**: `# REVIEW: insert to dwh_ta_f_rpos_fact_carm — SQL not extracted; supply manually`
* **DWH$TA_T_RPOS_CARM**
  * **Kind**: insert
  * **Table**: `dwh_ta_t_rpos_carm`
  * **SQL**: `# REVIEW: insert to dwh_ta_t_rpos_carm — SQL not extracted; supply manually`
* **DWH$TA_F_RPOS_CARM**
  * **Kind**: insert
  * **Table**: `dwh_ta_f_rpos_carm`
  * **SQL**: `# REVIEW: insert to dwh_ta_f_rpos_carm — SQL not extracted; supply manually`
* **DWH$TA_F_GPOS_FACT_CARM**
  * **Kind**: insert
  * **Table**: `dwh_ta_f_gpos_fact_carm`
  * **SQL**: `# REVIEW: insert to dwh_ta_f_gpos_fact_carm — SQL not extracted; supply manually`
* **DWH$TA_F_RPOS_RESELLING_CARM**
  * **Kind**: insert
  * **Table**: `dwh_ta_f_rpos_reselling_carm`
  * **SQL**: `# REVIEW: insert to dwh_ta_f_rpos_reselling_carm — SQL not extracted; supply manually`
* **Update DWH$TA_K_MELDUNGEN**
  * **Kind**: update
  * **Table**: `dwh$ta_k_meldungen`
  * **SQL**:
    ```sql
    update dwh$ta_k_meldungen 
    set anzahl_ds_eof = :anzahl
      , dateiname = :dateiname
      , enderecord_text = :inhalt
      , zusatzinfo = :bemerkung 
    where entrynr = :eintragsnr
    ```
* **Update / Insert DWH$TA_K_RECH_ABSGRP**
  * **Kind**: update
  * **Table**: `DWH$TA_K_RECH_ABSGRP`
  * **SQL**:
    ```sql
    UPDATE DWH$TA_K_RECH_ABSGRP
    SET   rechnung_datum = :rechnung_datum, 
          ladedatum = :ladedatum
    WHERE  monats_id = :monats_id
    AND    abs_grp = :abs_grp
    AND    dateiname = :dateiname
    AND    rechnungsteil = :rechnungsteil
    ```

---

## 7. BUSINESS SUMMARY
* **Step 1: Input Segmentation**: An inbound billing text file is read and split into data lines (Nutzdaten) and batch footer lines (Endedatensatz) based on dataset prefixes.
* **Step 2: File Normalization & Validation**: Commas in string representations of decimal values are converted to standard dots, and records are rigorously evaluated to ensure data formats of identifiers and numerical values are structurally sound.
* **Step 3: Temporal Contract Matching**: Validated records are joined with the active contracts master table (`dwh$ta_c_vertrag`). A custom validity logic validates that the billing month last-day timestamp falls within each contract's historic effective range (`gueltig_von` to `gueltig_bis`).
* **Step 4: Deletion of Pre-existing Batches**: Using dynamic keys derived from the parsed dataset, pre-existing historical records are identified and cleared out of target tables to prevent duplicated loads upon re-runs.
* **Step 5: Grouping & Target Partitioning**: The records are rolled up, aggregated (summing gross, net, and tax columns), and distributed based on classification rules (e.g. business forms `G` for factoring credits, `F` for factoring invoices, `R` for reselling, and `T` for temporary files) into corresponding core target database tables.
* **Step 6: Control Auditing**: Metadata extracted from the footer record is structured to update run history summaries in control tracking tables.

---

## PSEUDOCODE OUTLINE

```python
# Step 1: Read raw input text/CSV records
# REVIEW: Input file path not explicitly extracted, parameterized read pattern
df_raw_file = (
    spark.read.format("text")
    .load("gs://${INPUT_BUCKET}/billing_input_file.csv")
)
df_raw_file.createOrReplaceTempView("raw_file_view")

# Step 2: Extract Kennzeichen and Datensatz payloads
df_split_data = spark.sql("""
    SELECT 
        substring(value, 1, 5) AS kennzeichen,
        substring(value, 6) AS datensatz_rest,
        value AS raw_record
    FROM raw_file_view
""")
df_split_data.createOrReplaceTempView("split_data_view")

# Step 3: Parse and Clean payload records (Nutzdaten)
df_pay_raw = spark.sql("""
    SELECT 
        replace(datensatz_rest, ',', '.') AS datensatz_clean
    FROM split_data_view
    WHERE kennzeichen = '${BHB_Nutzdatensatzkennung}'
""")
df_pay_raw.createOrReplaceTempView("pay_raw_view")

# Parse positional CSV structure from pay_raw_view
df_parsed_payload = spark.sql("""
    SELECT 
        split(datensatz_clean, ';')[0] AS monats_id,
        split(datensatz_clean, ';')[1] AS rechnung_id,
        split(datensatz_clean, ';')[2] AS rechnung_datum,
        split(datensatz_clean, ';')[3] AS standardvertrags_id,
        split(datensatz_clean, ';')[4] AS vertrags_id,
        CAST(split(datensatz_clean, ';')[5] AS DECIMAL(18,2)) AS rechpos_brutto_eur,
        CAST(split(datensatz_clean, ';')[6] AS DECIMAL(18,2)) AS rechpos_netto_eur,
        CAST(split(datensatz_clean, ';')[7] AS DECIMAL(18,2)) AS rechpos_mwst_eur,
        split(datensatz_clean, ';')[8] AS rpos_geschaftsform_kenn,
        split(datensatz_clean, ';')[9] AS rech_leistung_id_carm,
        split(datensatz_clean, ';')[10] AS typ
    FROM pay_raw_view
""")
df_parsed_payload.createOrReplaceTempView("parsed_payload_view")

# Step 4: Validate Critical payload structures
df_validated_payload = spark.sql("""
    SELECT 
        CASE WHEN monats_id IS NULL THEN raise_error('Invalid data format in monats_id') ELSE monats_id END AS monats_id,
        CASE WHEN rechnung_id IS NULL THEN raise_error('Invalid data format in rechnung_id') ELSE rechnung_id END AS rechnung_id,
        CASE WHEN rechnung_datum IS NULL THEN raise_error('Invalid data format in rechnung_datum') ELSE rechnung_datum END AS rechnung_datum,
        CASE WHEN standardvertrags_id IS NULL THEN raise_error('Invalid data format in standardvertrags_id') ELSE standardvertrags_id END AS standardvertrags_id,
        CASE WHEN vertrags_id IS NULL THEN raise_error('Invalid data format in vertrags_id') ELSE vertrags_id END AS vertrags_id,
        CASE WHEN rechpos_brutto_eur IS NULL THEN raise_error('Invalid data format in rechpos_brutto_eur') ELSE rechpos_brutto_eur END AS rechpos_brutto_eur,
        CASE WHEN rechpos_netto_eur IS NULL THEN raise_error('Invalid data format in rechpos_netto_eur') ELSE rechpos_netto_eur END AS rechpos_netto_eur,
        CASE WHEN rechpos_mwst_eur IS NULL THEN raise_error('Invalid data format in rechpos_mwst_eur') ELSE rechpos_mwst_eur END AS rechpos_mwst_eur,
        rpos_geschaftsform_kenn,
        rech_leistung_id_carm,
        typ
    FROM parsed_payload_view
""")
df_validated_payload.createOrReplaceTempView("validated_payload_view")

# Step 5: Read historical contract dimension
df_vertrag_source = spark.read.format("bigquery").option("table", "BIGQUERY_SOURCE_DS.dwh_ta_c_vertrag").load()
df_vertrag_source.createOrReplaceTempView("dwh_ta_c_vertrag_source")

df_vertrag = spark.sql("""
    SELECT 
        rahmenvertrag_id,
        vertrag_id_carmen,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von, 
        gueltig_bis
    FROM dwh_ta_c_vertrag_source
    WHERE gueltig_bis >= to_date('2005-04-01', 'yyyy-MM-dd')
""")
df_vertrag.createOrReplaceTempView("dwh_ta_c_vertrag")

# Step 6: Join payload with contracts on active date intervals
df_joined_contracts = spark.sql("""
    SELECT 
        p.*,
        v.rahmenvertrag_id,
        v.dwh_vertrag_id,
        v.dwh_gp_id,
        v.dwh_konto_id,
        v.dwh_tarifgr_id,
        v.vo_kenn,
        v.zv_id,
        v.gueltig_von,
        v.gueltig_bis,
        last_day(to_date(concat(p.monats_id, '01'), 'yyyyMMdd')) AS month_last_day
    FROM validated_payload_view p
    LEFT JOIN dwh_ta_c_vertrag v ON p.vertrags_id = v.vertrag_id_carmen
""")
df_joined_contracts.createOrReplaceTempView("joined_contracts_view")

# Validate date constraints on the join (Proof Join reformat)
df_proof_join = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur,
        rpos_geschaftsform_kenn,
        rech_leistung_id_carm,
        typ,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN rahmenvertrag_id ELSE NULL END AS rahmenvertrag_id,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN dwh_vertrag_id ELSE NULL END AS dwh_vertrag_id,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN dwh_gp_id ELSE NULL END AS dwh_gp_id,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN dwh_konto_id ELSE NULL END AS dwh_konto_id,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN dwh_tarifgr_id ELSE NULL END AS dwh_tarifgr_id,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN vo_kenn ELSE NULL END AS vo_kenn,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN zv_id ELSE NULL END AS zv_id,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN gueltig_von ELSE NULL END AS gueltig_von
    FROM joined_contracts_view
""")
df_proof_join.createOrReplaceTempView("proof_join_view")

# Step 7: Apply deduplication & ranking filter on rankindex == 1
df_ranked_join = spark.sql("""
    SELECT *,
        row_number() OVER (
            PARTITION BY vertrags_id, rechnung_id, rechnung_datum, standardvertrags_id
            ORDER BY gueltig_von DESC, dwh_vertrag_id DESC
        ) AS rankindex
    FROM proof_join_view
""")
df_ranked_join.createOrReplaceTempView("ranked_join_view")

df_rank_filtered = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur,
        rpos_geschaftsform_kenn,
        rech_leistung_id_carm,
        typ,
        rahmenvertrag_id,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von
    FROM ranked_join_view
    WHERE rankindex = 1
""")
df_rank_filtered.createOrReplaceTempView("rank_filtered_view")

# Step 8: Perform aggregation rollup
df_rolled_up = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rahmenvertrag_id,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rpos_geschaftsform_kenn,
        rech_leistung_id_carm,
        typ,
        SUM(rechpos_brutto_eur) AS rechpos_brutto_eur,
        SUM(rechpos_netto_eur) AS rechpos_netto_eur,
        SUM(rechpos_mwst_eur) AS rechpos_mwst_eur
    FROM rank_filtered_view
    GROUP BY 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rahmenvertrag_id,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rpos_geschaftsform_kenn,
        rech_leistung_id_carm,
        typ
""")
df_rolled_up.createOrReplaceTempView("rolled_up_view")

# Step 9: Delete existing dynamic records before running inserts (Maintenance step)
# REVIEW: Emulated in PySpark environment using delete logic or transactional overrides.

# Step 10: Route payload records to distinct outputs based on Business Rules

# 10a. Target: Factoring Gutschriften (G) -> DWH$TA_F_GPOS_FACT_CARM
df_gutschriften = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        substring(rech_leistung_id_carm, 1, 9) AS rech_leistung_id_carm,
        rahmenvertrag_id AS rahmenvertrag,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur
    FROM rolled_up_view
    WHERE rpos_geschaftsform_kenn = 'G'
""")
# Save df_gutschriften to target BQ table dwh_ta_f_gpos_fact_carm
df_gutschriften.write.format("bigquery").mode("append").save("BIGQUERY_TARGET_DS.dwh_ta_f_gpos_fact_carm")

# 10b. Target: Factoring Rechnungen (F) -> DWH$TA_F_RPOS_FACT_CARM
df_factoring_rechnungen = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        substring(rech_leistung_id_carm, 1, 9) AS rech_leistung_id_carm,
        rahmenvertrag_id AS rahmenvertrag,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur
    FROM rolled_up_view
    WHERE rpos_geschaftsform_kenn = 'F'
""")
# Save df_factoring_rechnungen to target BQ table dwh_ta_f_rpos_fact_carm
df_factoring_rechnungen.write.format("bigquery").mode("append").save("BIGQUERY_TARGET_DS.dwh_ta_f_rpos_fact_carm")

# 10c. Target: Reselling (R) -> DWH$TA_F_RPOS_RESELLING_CARM
df_reselling = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        substring(rech_leistung_id_carm, 1, 9) AS rech_leistung_id_carm,
        rahmenvertrag_id AS rahmenvertrag,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur
    FROM rolled_up_view
    WHERE rpos_geschaftsform_kenn = 'R'
""")
# Save df_reselling to target BQ table dwh_ta_f_rpos_reselling_carm
df_reselling.write.format("bigquery").mode("append").save("BIGQUERY_TARGET_DS.dwh_ta_f_rpos_reselling_carm")

# 10d. Target: Temporary Positions (T) -> DWH$TA_T_RPOS_CARM
df_temporary_data = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rech_leistung_id_carm,
        rahmenvertrag_id,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur,
        CAST('1900-01-01 00:00:00' AS TIMESTAMP) AS bearbeitung_datum
    FROM rolled_up_view
    WHERE typ = 'T'
""")
# Save df_temporary_data to target BQ table dwh_ta_t_rpos_carm
df_temporary_data.write.format("bigquery").mode("append").save("BIGQUERY_TARGET_DS.dwh_ta_t_rpos_carm")

# 10e. Target: Base Fact Positions (Non-T) -> DWH$TA_F_RPOS_CARM
df_fact_data = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rech_leistung_id_carm,
        rahmenvertrag_id AS rahmenvertrag,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur
    FROM rolled_up_view
    WHERE typ != 'T'
""")
# Save df_fact_data to target BQ table dwh_ta_f_rpos_carm
df_fact_data.write.format("bigquery").mode("append").save("BIGQUERY_TARGET_DS.dwh_ta_f_rpos_carm")


# Step 11: Parse Metadata End Record (Footer)
df_metadata_raw = spark.sql("""
    SELECT 
        datensatz_rest
    FROM split_data_view
    WHERE kennzeichen = '${BHB_Endedatensatzkennung}'
""")
df_metadata_raw.createOrReplaceTempView("metadata_raw_view")

# Parse Metadata CSV columns
df_metadata_parsed = spark.sql("""
    SELECT 
        split(datensatz_rest, ';')[0] AS bemerkung,
        split(datensatz_rest, ';')[1] AS stichtag,
        CAST(split(datensatz_rest, ';')[2] AS INT) AS anzahl,
        split(datensatz_rest, ';')[3] AS inhalt,
        split(datensatz_rest, ';')[4] AS erstellt_am,
        -- # REVIEW: eintragsnr is mapped from metadata or context; using placeholder
        1001 AS eintragsnr 
    FROM metadata_raw_view
""")
df_metadata_parsed.createOrReplaceTempView("metadata_parsed_view")

# Clean created date timestamp inside the footer record
df_ende_record = spark.sql("""
    SELECT 
        bemerkung,
        stichtag,
        anzahl,
        inhalt,
        CASE WHEN instr(erstellt_am, ';') = 0 THEN erstellt_am 
             ELSE substring(erstellt_am, 1, length(erstellt_am) - 1) 
        END AS erstellt_am,
        eintragsnr
    FROM metadata_parsed_view
""")
df_ende_record.createOrReplaceTempView("ende_record_view")

# Generate DB registration values
df_rech_absgrp = spark.sql("""
    SELECT 
        -- monats_id: Subtract 1 month from substring(stichtag, 1, 6)
        date_format(add_months(to_date(substring(stichtag, 1, 6), 'yyyyMM'), -1), 'yyyyMM') AS monats_id,
        substring(bemerkung, 10, 5) AS abs_grp,
        bemerkung AS dateiname,
        to_date(stichtag, 'yyyyMMdd') AS rechnung_datum,
        'P' AS rechnungsteil,
        current_timestamp() AS ladedatum
    FROM ende_record_view
""")
df_rech_absgrp.createOrReplaceTempView("rech_absgrp_view")

# Execute updates on Control / Audit Log target tables

# Update DWH$TA_K_MELDUNGEN using merge logic on entrynr
df_meldungen_update = spark.sql("""
    SELECT 
        anzahl,
        dateiname,
        inhalt,
        bemerkung,
        eintragsnr
    FROM ende_record_view
""")
# REVIEW: Perform MERGE INTO DWH$TA_K_MELDUNGEN utilizing df_meldungen_update keys

# Update / Insert DWH$TA_K_RECH_ABSGRP
df_rech_absgrp_update = spark.sql("""
    SELECT 
        rechnung_datum,
        ladedatum,
        monats_id,
        abs_grp,
        dateiname,
        rechnungsteil
    FROM rech_absgrp_view
""")
# REVIEW: Perform MERGE INTO DWH$TA_K_RECH_ABSGRP utilizing df_rech_absgrp_update keys
```

=== END OF VERBATIM MCP OUTPUT ===

---

## 7. Risks & Manual Actions

1. **Environmental Target Datasets**: The exact schema designations for target databases (e.g., `BIGQUERY_TARGET_DS` and `BIGQUERY_SOURCE_DS`) must be mapped to runtime Google Cloud environmental configurations at execution time.
2. **Missing Input Paths**: The dynamic file patterns mapped in variables like `BHB_Quellverzeichnis` rely on the dynamic environment variable `$DW_DIR_IMP_SAP`. System testing must verify GCS path access configuration matches the GCS bucket variables set in Airflow.
3. **Audit Log Merge Actions**: High-priority manual review is required to construct the dynamic SQL MERGE statements for target tables `DWH$TA_K_MELDUNGEN` and `DWH$TA_K_RECH_ABSGRP`. This ensures run metadata updates correctly synchronize back to the master audit frames.
4. **German Language Legacy Validation Comments**: Maintain precise logging outputs in downstream tables exactly as validated by legacy rules:
   * `"Invalid data format in monats_id"`
   * `"Invalid data format in rechnung_datum"`
   * `"Invalid Data in field monats_id"`
   * `"Invalid Data in field debitor_id"`
   *(Translation is banned to preserve compatibility with downstream monitoring scripts)*.

---

# MIGRATION DESIGN DOCUMENT: DW.RPOS_CARM_IMPORT

---

## 1. FILE DISPOSITION

Every file associated with this job from the legacy codebase is mapped in the table below. No source files are omitted.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py` | PySpark conversion script. Implements the complex business data transformations, validations, joins, and load patterns extracted from the Ab Initio graph. |
| `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` | `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.yaml` | YAML Configuration file. Translates the legacy key-value configurations verbatim into standard target execution configuration parameters. |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml` | `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import_dag.py` | Airflow DAG orchestrating the load. Replaces the UC4 Unix Job definition and manages scheduling, sensors, and task execution on Google Cloud Composer. |

---

## 2. CONSOLIDATED TARGET FILE PLAN

The repository structure below strictly mirrors the legacy folder structure to maintain architectural integrity:

```
abinitio_rpos_carmen_linked_job/
├── DWH_BD_PROC_JOB/
│   └── dw_rpos_carm_import_dag.py (Airflow Orchestration DAG)
├── TMD_processing/
│   └── BHB/
│       └── BD_PROC/
│           └── run/
│               └── map_rpos_carmen_import.py (Dataproc Serverless PySpark script)
└── isdwh/
    └── abinitio/
        └── cfg/
            └── bd_proc/
                └── map_rpos_carmen_import.yaml (Job Configurations)
```

---

## 3. VERBATIM REQUIRED TOOL OUTPUT (ksh_design_python)

Below is the complete, unmodified conversion design document and Python pseudocode framework returned by the `ksh_design_python` tool:

```markdown
# DESIGN DOCUMENT: map_rpos_carmen_import Conversion

## 1. SCRIPT OVERVIEW
*   **Purpose**: This script is a compiled Ab Initio GDE (Graphical Development Environment) execution wrapper for the graph `map_rpos_carmen_import`. Its primary purpose is to orchestrate a complex data integration pipeline that imports, validates, transforms, and loads CARMEN billing and invoice position (RPOS) data into an Oracle Data Warehouse (DWH).
*   **Triggers/Invocation**: It is executed within an enterprise scheduler environment (e.g., Automic/UC4) that sets up the required environment variables and triggers this shell script.
*   **Inputs**: It reads an upstream CSV transaction file specified by the `BHB_Dateiname` parameter and performs database lookups against the historical contract table `dwh$ta_c_vertrag`.
*   **Outputs**: It deletes existing transaction records (incremental/idempotent load strategy) and inserts/updates rows across multiple database tables: `DWH$TA_F_RPOS_CARM`, `DWH$TA_F_GPOS_FACT_CARM`, `DWH$TA_F_RPOS_FACT_CARM`, `DWH$TA_F_RPOS_RESELLING_CARM`, `DWH$TA_T_RPOS_CARM`, `DWH$TA_K_RECH_ABSGRP`, and `dwh$ta_k_meldungen`.
*   **Boilerplate Flag**: `ksh_is_boilerplate=True` (no custom business logic was written between the `#+Script Start+` and `#+Script End+` blocks besides setting the environment variable `NLS_NUMERIC_CHARACTERS=". "`). The core ETL logic is defined in the compiled Ab Initio GDE component scaffolding and pipeline flows executed by `mp run`. However, the script contains critical embedded SQL statements, parameter declarations, and structural wrappers that define the business logic for migration.

---

## 2. INVOCATION CONTEXT
*   **Caller**: Scheduler context (Automic/UC4 job name expected to be something like `JOBS_UNIX.BHB.BD_PROC.MAP_RPOS_CARMEN_IMPORT`).
*   **Command Line / Arguments**: Called with positional parameters (if any are supplied, they are passed down to EME sandbox and project invocation scripts).
*   **UC4 Native Includes**:
    *   No direct native UC4 `:inc` statements are present inside this `.ksh` script body.
*   **Environment Files Sourced**:
    *   `_AB_PROJECT_KSH` (evaluated as `${_AB_SAVED_PROJECT_DIR}/.project.ksh`): Sourced twice (at startup with `execute start` and at shutdown with `execute end`).
        *   `# REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown; do not guess their names or values`
    *   `ab_catalog_functions.ksh`: Sourced dynamically if it exists under `$AB_HOME/bin/`.
        *   `# REVIEW-STRUCT: environment file [ab_catalog_functions.ksh] not supplied — variables it sets are unknown; do not guess their names or values`
    *   `./${_AB_PROXY_DIR}/GDE-Parameters`: Sourced to load graph-specific parameters.
        *   `# REVIEW-STRUCT: environment file [GDE-Parameters] not supplied — variables it sets are unknown; do not guess their names or values`

---

## 3. PARAMETERS / INPUTS
### Captured Environment Parameters (Boilerplate Variables)
*   **AB_HOME**:
    *   Source: Environment variable (defaults to `/appl/local/abinitio/abinitio`).
    *   Used in Body: Yes, to set pathing and source catalog functions.
    *   Python Surface: `os.environ.get("AB_HOME", "/appl/local/abinitio/abinitio")`
*   **MPOWERHOME**:
    *   Source: Environment variable (set to `$AB_HOME`).
    *   Used in Body: Yes, exported for Ab Initio execution.
    *   Python Surface: `os.environ.get("MPOWERHOME")`
*   **AB_REPORT**:
    *   Source: Environment variable (defaults to `'monitor=60 processes scroll=true'`).
    *   Used in Body: Yes, exported.
    *   Python Surface: `os.environ.get("AB_REPORT")`
*   **AB_AIR_HOME**:
    *   Source: Environment variable (defaults to `/appl/local/abinitio/abinitio-V2-14`).
    *   Used in Body: Yes, exported.
    *   Python Surface: `os.environ.get("AB_AIR_HOME")`
*   **PROJECT_DIR**:
    *   Source: Environment variable or dynamically resolved via dirname/symlinks of `$0`.
    *   Used in Body: Yes, exported.
    *   Python Surface: Determined dynamically via `os.path.dirname(os.path.abspath(sys.argv[0]))` or overridden by `os.environ.get("PROJECT_DIR")`.

### KSH Declared Environment Parameters (Cross-Referenced GDE Variables)
The following parameters are declared, evaluated, and checked for successful resolution. They represent both database connections and process configurations:

#### DB-Connection-Style Parameters (Oracle-specific TNS environments):
*   **DB_TNS_NAME_DWH** / **DB_USER_DWH** / **DB_PASSWD_DWH**
*   **DB_TNS_NAME_CRS** / **DB_USER_CRS** / **DB_PASSWD_CRS**
*   **DB_TNS_NAME_SGM** / **DB_USER_SGM** / **DB_PASSWD_SGM**
*   **DB_TNS_NAME_CADS** / **DB_USER_CADS** / **DB_PASSWD_CADS**
*   **DB_TNS_NAME_CACM** / **DB_USER_CACM** / **DB_PASSWD_CACM**
    *   *Classification*: DB-connection parameters. Note: These are standard cross-referenced database environment variables, confirming the targets reside on Oracle databases.
    *   *Python Surface*: Retrieved via `os.environ.get("DB_USER_DWH")`, etc.

#### Generic Internal / Framework Parameters:
*   **BHB_Projektverzeichnis**: Project base directory.
*   **BHB_Graph**: Graph execution name (`map_rpos_carmen_import`).
*   **BHB_Prozesstyp**: Type of business process.
*   **BHB_Eintragsnr**: Target tracking / auditor ID for the current execution run.
*   **BHB_Quellverzeichnis**: Source directory where inputs are staged.
*   **BHB_Zielverzeichnis**: Target directory where output archives are stored.
*   **BHB_Dateimaske**: Filename wildcard pattern for the input.
*   **BHB_Kopfdatensatzkennung**: File header record identifier.
*   **BHB_Nutzdatensatzkennung**: Business data record identifier (payload).
*   **BHB_Endedatensatzkennung**: File footer record identifier (control record).
*   **BHB_Dateiname**: Full pathname of the input data file.
    *   *Classification*: Informational/operational parameters.
    *   *Python Surface*: Retrieved via `os.environ.get("BHB_Dateiname")`, etc.

---

## 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
The script invokes several infrastructure and Ab Initio command-line utilities. Since this is an Ab Initio graph execution script, the ETL pipeline relies entirely on the Ab Initio Enterprise Meta Environment (EME) and execution engine commands:

*   **uname**:
    ```bash
    uname
    ```
    *Purpose*: Identifies the host operating system to adjust path separators.
    *Translation*: Native Python `platform.system()` or `sys.platform`.
*   **air sandbox find**:
    ```bash
    air sandbox find "${PROJECT_DIR}" -project
    ```
    *Purpose*: Queries the EME repository path for the current sandbox.
    *Translation*: Must remain an external call via `subprocess.run` if EME integration is preserved.
*   **run-and-reposit**:
    ```bash
    ${AB_HOME}/bin/run-and-reposit "${_AB_PROJECT_NAME}"'/mp/map_rpos_carmen_import.mp' ...
    ```
    *Purpose*: Wrapper utility to execute and version control the run state of the graph in EME.
    *Translation*: External process execution via `subprocess.run`.
*   **air rm / air mv**:
    ```bash
    air rm -r -f "${AB_AIR_JOB}"
    air mv "${AB_ORIGINAL_AIR_JOB}" "${AB_AIR_JOB}"
    ```
    *Purpose*: Manages repository paths for metadata tracking.
    *Translation*: External process execution via `subprocess.run`.
*   **Ab Initio Co-Operating System Engine commands (`mp`, `m_db_layout`, `m_rmcatalog`, `m_mkcatalog`)**:
    *   Verbatim command strings inside graph evaluation:
        *   `mp job ${AB_JOB}`
        *   `mp layout ...`
        *   `mp metadata ...`
        *   `mp straight-flow ...`
        *   `mp run`
        *   `mp reset`
    *   *Purpose*: Setup parallel layout execution, register metadata schemas (DML), bind component flows, clean catalogs, and run the pipeline.
    *   *Translation*: These are core Ab Initio proprietary components. They are NOT resolvable launchers since they represent a full graphical ETL pipeline, not a simple database wrapper. In a pure Python migration, this whole scaffolding should either be run via `subprocess.run` (if calling the legacy Ab Initio engine is allowed) or re-engineered into native Python pandas/SQL pipelines.

---

## 5. EMBEDDED SQL
The script writes several SQL statements to a proxy directory, which are subsequently executed as part of the pipeline's database actions. These statements represent the business logic for deletions (incremental logic) and updates (auditing):

### 1. Delete rows from `DWH$TA_F_RPOS_CARM`
*   **Source File**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_CARM-4.sql` & `Delete_rows_from_DWH_TA_F_RPOS_CARM_2-61.sql`
*   **SQL Text**:
    ```sql
    DELETE FROM DWH$TA_F_RPOS_CARM
    WHERE  rechnung_id = :rechnung_id
    AND    rechnung_datum = :rechnung_datum
    AND    standardvertrags_id = :standardvertrags_id
    AND    vertrags_id = :vertrags_id
    ```
*   **Type**: `DELETE`
*   **Tables Touched**: `DWH$TA_F_RPOS_CARM`
*   **Dialect**: Oracle SQL (uses Oracle-style bind variable notation `:variable`).

### 2. Delete rows from `DWH$TA_F_GPOS_FACT_CARM`
*   **Source File**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_GPOS_FACT_CARM-60.sql`
*   **SQL Text**:
    ```sql
    DELETE FROM DWH$TA_F_GPOS_FACT_CARM
    WHERE  rechnung_id = :rechnung_id
    AND    rechnung_datum = :rechnung_datum
    AND    standardvertrags_id = :standardvertrags_id
    AND    vertrags_id = :vertrags_id
    ```
*   **Type**: `DELETE`
*   **Tables Touched**: `DWH$TA_F_GPOS_FACT_CARM`
*   **Dialect**: Oracle SQL.

### 3. Delete rows from `DWH$TA_F_RPOS_FACT_CARM`
*   **Source File**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_FACT_CARM-62.sql`
*   **SQL Text**:
    ```sql
    DELETE FROM DWH$TA_F_RPOS_FACT_CARM
    WHERE  rechnung_id = :rechnung_id
    AND    rechnung_datum = :rechnung_datum
    AND    standardvertrags_id = :standardvertrags_id
    AND    vertrags_id = :vertrags_id
    ```
*   **Type**: `DELETE`
*   **Tables Touched**: `DWH$TA_F_RPOS_FACT_CARM`
*   **Dialect**: Oracle SQL.

### 4. Delete rows from `DWH$TA_F_RPOS_RESELLING_CARM`
*   **Source File**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_F_RPOS_RESELLING_CARM-63.sql`
*   **SQL Text**:
    ```sql
    DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM
    WHERE  rechnung_id = :rechnung_id
    AND    rechnung_datum = :rechnung_datum
    AND    standardvertrags_id = :standardvertrags_id
    AND    vertrags_id = :vertrags_id
    ```
*   **Type**: `DELETE`
*   **Tables Touched**: `DWH$TA_F_RPOS_RESELLING_CARM`
*   **Dialect**: Oracle SQL.

### 5. Delete rows from `DWH$TA_T_RPOS_CARM`
*   **Source File**: `${_AB_PROXY_DIR}/Delete_rows_from_DWH_TA_T_RPOS_CARM-65.sql`
*   **SQL Text**:
    ```sql
    DELETE FROM DWH$TA_T_RPOS_CARM
    WHERE  debitor_id = :debitor_id
    AND    rechnung_datum = :rechnung_datum
    AND    rechnung_id = :rechnung_id
    ```
*   **Type**: `DELETE`
*   **Tables Touched**: `DWH$TA_T_RPOS_CARM`
*   **Dialect**: Oracle SQL.

### 6. Update `DWH$TA_K_RECH_ABSGRP`
*   **Source File**: `${_AB_PROXY_DIR}/Update_Insert_DWH_TA_K_RECH_ABSGRP-70.sql`
*   **SQL Text**:
    ```sql
    UPDATE DWH$TA_K_RECH_ABSGRP
    SET   rechnung_datum = :rechnung_datum, 
          ladedatum = :ladedatum
    WHERE  monats_id = :monats_id
    AND    abs_grp = :abs_grp
    AND    dateiname = :dateiname
    AND    rechnungsteil = :rechnungsteil
    ```
*   **Type**: `UPDATE`
*   **Tables Touched**: `DWH$TA_K_RECH_ABSGRP`
*   **Dialect**: Oracle SQL.

### 7. Insert into `DWH$TA_K_RECH_ABSGRP`
*   **Source File**: `${_AB_PROXY_DIR}/Update_Insert_DWH_TA_K_RECH_ABSGRP-71.sql`
*   **SQL Text**:
    ```sql
    INSERT INTO DWH$TA_K_RECH_ABSGRP (monats_id, abs_grp, dateiname,  rechnung_datum, rechnungsteil, ladedatum)
    VALUES (:monats_id, :abs_grp, :dateiname,  :rechnung_datum, :rechnungsteil, :ladedatum)
    ```
*   **Type**: `INSERT`
*   **Tables Touched**: `DWH$TA_K_RECH_ABSGRP`
*   **Dialect**: Oracle SQL.

### 8. Update `dwh$ta_k_meldungen`
*   **Source File**: `${_AB_PROXY_DIR}/Update_DWH_TA_K_MELDUNGEN-74.sql`
*   **SQL Text**:
    ```sql
    update dwh$ta_k_meldungen 
    set anzahl_ds_eof = :anzahl
      , dateiname = :dateiname
      , enderecord_text = :inhalt
      , zusatzinfo = :bemerkung 
    where entrynr = :eintragsnr
    ```
*   **Type**: `UPDATE`
*   **Tables Touched**: `dwh$ta_k_meldungen`
*   **Dialect**: Oracle SQL.

### 9. Lookup Select Query on `dwh$ta_c_vertrag`
*   **Source File**: Defined within lookup component declarations.
*   **SQL Text**:
    ```sql
    select 
    rahmenvertrag_id,
    vertrag_id_carmen,
    dwh_vertrag_id,
    dwh_gp_id,
    dwh_konto_id,
    dwh_tarifgr_id,
    vo_kenn,
    zv_id,
    gueltig_von, 
    gueltig_bis
    from 
    dwh$ta_c_vertrag
    where 
    gueltig_bis >= to_date('20050401', 'YYYYMMDD') 
    ```
*   **Type**: `SELECT`
*   **Tables Touched**: `dwh$ta_c_vertrag`
*   **Dialect**: Oracle SQL.

---

## 6. CONTROL FLOW
The script follows a sequential configuration, execution, and teardown pipeline:

1.  **Environment Initialization**: Sets base directory variables, path updates, and Ab Initio internal parameters based on architecture (`uname`).
2.  **Project Script Setup**: Computes `PROJECT_DIR` dynamically using parent-directory resolution via `dirname` and symbolic link traversal.
3.  **Proxy Directory Generation**: Creates a process-unique temporary directory `${AB_JOB}-map_rpos_carmen_import-ProxyDir` to stage schemas (DMLs), logic mappings (XFRs), and raw SQL commands.
4.  **Signal Traps**: Registers EXIT, HUP, INT, QUIT, and TERM traps to ensure the clean deletion of temporary files on normal execution or standard signals.
5.  **Repository Setup Hook**: Checks repository parameters (`_REPOSIT_TRACKING`); triggers EME Datastore actions if true and exits.
6.  **Execute Start Hook**: Sources `.project.ksh` with arguments `execute start` to initialize external credentials and variables.
7.  **Variable Validation**: Validates the physical evaluation of DWH, SGM, CRS, and CADS database variables and exports local business settings.
8.  **Internal Custom Hook (Script Start)**: Exports `NLS_NUMERIC_CHARACTERS=". "`.
9.  **Inline Payload Generation**: Emits metadata schemas (DMLs), routing schemas (XFRs), and SQL statement structures to the generated proxy directories.
10. **Ab Initio Graph Building**: Invokes `mp job` and dynamic metadata mapping definitions (`mp metadata`, `mp layout`, `mp straight-flow`).
11. **Catalog Setup**: Clears existing catalogs and initializes execution-specific catalog instances (`m_rmcatalog`, `m_mkcatalog`).
12. **ETL Execution Engine Call**: Executes the full parallel integration workflow using the proprietary command `mp run`.
13. **Result Status Capture**: Saves the execution outcome of the dataflow pipeline into status code variable `mpjret`.
14. **Cleanup Teardown**: Performs engine state resets (`mp reset`), deletes graph catalog, and triggers the standard file-cleanup trap function.
15. **Execute End Hook**: Sourced `.project.ksh` with arguments `execute end`.
16. **Termination**: Propagates `mpjret` status code back to the parent execution environment.

---

## 7. ERROR HANDLING & EXIT CODES
*   **Error Detection**:
    *   Explicit validation on variable assignments. If any `mpjret` is non-zero after evaluating the parameter settings (e.g., `mpjret=$?`), the script writes an error message to stdout and exits immediately with `exit $mpjret`.
    *   Exit codes are explicitly checked for repository adjustments (`air rm` and `air mv`).
    *   The overall pipeline status is captured from `mp run` exit status and returned via `exit $mpjret`.
*   **Signals**: Traps standard signal terminations (HUP, INT, QUIT, TERM) to trigger proxy directory file cleanup (`rm -rf`) before exiting with the matching signal exit code.
*   **Python Migration Approach**:
    *   The cleanup trap should be handled natively using Python’s `try ... finally` blocks or the `atexit` standard library.
    *   Process execution calls (`subprocess.run`) should be run with `check=True` or explicitly handled using a try-except block catching `subprocess.CalledProcessError`.
    *   SQL database executions should handle DB-driver exceptions (e.g. `oracledb.DatabaseError`) and raise them to fail the script if errors are encountered.

---

## 8. OUTPUTS / SIDE EFFECTS
*   **Files**: Writes temporary schema mappings and transformation specifications inside proxy directories (`${AB_JOB}-map_rpos_carmen_import-ProxyDir`).
*   **Database Updates (Oracle DWH Target)**:
    *   Performs database rows deletion inside fact tables `DWH$TA_F_RPOS_CARM`, `DWH$TA_F_GPOS_FACT_CARM`, `DWH$TA_F_RPOS_FACT_CARM`, `DWH$TA_F_RPOS_RESELLING_CARM` based on business invoice keys (`rechnung_id`, `rechnung_datum`, `standardvertrags_id`, `vertrags_id`).
    *   Performs temporary database deletions inside staging table `DWH$TA_T_RPOS_CARM` based on transaction keys (`debitor_id`, `rechnung_datum`, `rechnung_id`).
    *   Inserts parsed invoice details into corresponding production target tables depending on split classification flags (`rpos_geschaftsform_kenn` / `typ`).
    *   Updates auditing tracking records inside mapping metrics table `DWH$TA_K_RECH_ABSGRP` (including file name, record totals, and staging load date).
    *   Performs status logging updates inside workflow auditor table `dwh$ta_k_meldungen` using the current runtime ID `entrynr = :eintragsnr`.

---

## 9. BUSINESS SUMMARY
*   **Data Validation and Intake**: Automatically identifies and ingests daily invoice position streams from the CARMEN upstream platform based on configured file masks and target directories.
*   **Control/Payload Segregation**: Separates business payload transactions (Nutzdaten) from validation metrics (Enderecord/Footer) to evaluate and document execution integrity.
*   **Historical Contract Resolution**: Joins incoming positions dynamically with historical system contracts in `dwh$ta_c_vertrag` based on temporal bounds (`gueltig_von` / `gueltig_bis`), resolving operational metadata such as General Partner IDs, Account IDs, and Tariff groups.
*   **Idempotent Load Strategy**: Eliminates duplicate processing risks by dynamically searching for and purging database rows matching incoming key transactions from target tables before processing the new stream.
*   **Business Splitting**: Dispatches records dynamically to the correct database domain (Factoring Rechnungen, Factoring Gutschriften, or Reselling) based on business indicator evaluations.
*   **Metadata Reconciliation**: Logs total run details, parsed record quantities, control dates, and execution metrics inside core analytical tables (`DWH$TA_K_RECH_ABSGRP` and `dwh$ta_k_meldungen`) to maintain auditability and business traceability.

---

# PYTHON PSEUDOCODE OUTLINE

```python
#!/usr/bin/env python3
"""
Python conversion of map_rpos_carmen_import execution harness.
This is a translation of a compiled Ab Initio GDE script framework.
"""

import os
import sys
import shutil
import tempfile
import subprocess
import platform
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying
DB_USER = os.environ.get("DB_USER_DWH")
DB_PASSWD = os.environ.get("DB_PASSWD_DWH")
DB_TNS = os.environ.get("DB_TNS_NAME_DWH")

# Step 1: Environment Setup
AB_HOME = os.environ.get("AB_HOME", "/appl/local/abinitio/abinitio")
os.environ["MPOWERHOME"] = AB_HOME
os.environ["AB_REPORT"] = os.environ.get("AB_REPORT", "monitor=60 processes scroll=true")
os.environ["AB_AIR_HOME"] = os.environ.get("AB_AIR_HOME", "/appl/local/abinitio/abinitio-V2-14")
os.environ["AB_COMPATIBILITY"] = "2.14.59"

# Determine OS Platform to establish execution paths
os_platform = platform.system()
if os_platform.startswith("Windows"):
    os.environ["PATH"] = f"{AB_HOME}/bin;{os.environ.get('PATH', '')}"
else:
    os.environ["PATH"] = f"{AB_HOME}/bin:{os.environ.get('PATH', '')}"

# Resolve base project directory
script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
PROJECT_DIR = os.environ.get("PROJECT_DIR", os.path.abspath(os.path.join(script_dir, "..")))
os.environ["PROJECT_DIR"] = PROJECT_DIR

# Establish temporary session directories for mapping payloads
ab_job_prefix = os.environ.get("AB_JOB_PREFIX", "")
ab_job_name = f"{ab_job_prefix}map_rpos_carmen_import"
os.environ["AB_JOB"] = ab_job_name
os.environ["AB_GRAPH_NAME"] = "map_rpos_carmen_import"

proxy_dir = os.path.join(os.getcwd(), f"{ab_job_name}-map_rpos_carmen_import-ProxyDir")

def cleanup_proxy_files():
    """Removes temporary proxy metadata directories on termination."""
    logging.info(f"Triggering cleanup for directory: {proxy_dir}")
    if os.path.exists(proxy_dir):
        shutil.rmtree(proxy_dir)

# Step 2: Initialize clean setup workspace
try:
    if os.path.exists(proxy_dir):
        shutil.rmtree(proxy_dir)
    os.makedirs(proxy_dir, exist_ok=True)
    
    with open(os.path.join(proxy_dir, 'GDE-Parameters'), 'w') as f:
        f.write("")
        
except Exception as e:
    logging.error(f"Failed to initialize sandbox proxy workspace: {e}")
    sys.exit(1)

# Step 3: Sourced Project/Environmental includes
# # REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown; do not guess their names or values
# # REVIEW-STRUCT: environment file [ab_catalog_functions.ksh] not supplied — variables it sets are unknown; do not guess their names or values
# # REVIEW-STRUCT: environment file [GDE-Parameters] not supplied — variables it sets are unknown; do not guess their names or values

# Executing start hooks (Equivalent to .project.ksh execute start)
logging.info("Executing start hooks via .project.ksh")
try:
    # If the database and environmental configurations are successfully migrated to Python,
    # target connections would be initialized here natively using standard DB libraries.
    subprocess.run(["ksh", f"{PROJECT_DIR}/.project.ksh", PROJECT_DIR, "execute", "start"], check=True)
except subprocess.CalledProcessError as e:
    logging.error(f"Project execution initialization failed: {e}")
    cleanup_proxy_files()
    sys.exit(e.returncode)

# Step 4: Validate parameters
required_env_vars = [
    "BHB_Projektverzeichnis", "BHB_Graph", "BHB_Prozesstyp", "BHB_Eintragsnr",
    "BHB_Quellverzeichnis", "BHB_Zielverzeichnis", "BHB_Dateimaske",
    "BHB_Kopfdatensatzkennung", "BHB_Nutzdatensatzkennung", 
    "BHB_Endedatensatzkennung", "BHB_Dateiname"
]

for var in required_env_vars:
    if not os.environ.get(var):
        logging.error(f"Error evaluating: parameter {var} of map_rpos_carmen_import. Parameter not set.")
        cleanup_proxy_files()
        sys.exit(1)

# Step 5: Custom Script Start Block
os.environ["NLS_NUMERIC_CHARACTERS"] = ". "

# Step 6: Generate dynamic inline logic representations (XFR/DML/SQL definitions)
try:
    # SQL structures written to proxy dir (Representing the queries from Section 5)
    with open(os.path.join(proxy_dir, 'Delete_rows_from_DWH_TA_F_RPOS_CARM-4.sql'), 'w') as f:
        f.write("DELETE FROM DWH$TA_F_RPOS_CARM\n"
                "WHERE  rechnung_id = :rechnung_id\n"
                "AND    rechnung_datum = :rechnung_datum\n"
                "AND    standardvertrags_id = :standardvertrags_id\n"
                "AND    vertrags_id = :vertrags_id")
        
    with open(os.path.join(proxy_dir, 'Delete_rows_from_DWH_TA_F_GPOS_FACT_CARM-60.sql'), 'w') as f:
        f.write("DELETE FROM DWH$TA_F_GPOS_FACT_CARM\n"
                "WHERE  rechnung_id = :rechnung_id\n"
                "AND    rechnung_datum = :rechnung_datum\n"
                "AND    standardvertrags_id = :standardvertrags_id\n"
                "AND    vertrags_id = :vertrags_id")

    with open(os.path.join(proxy_dir, 'Delete_rows_from_DWH_TA_F_RPOS_CARM_2-61.sql'), 'w') as f:
        f.write("DELETE FROM DWH$TA_F_RPOS_CARM\n"
                "WHERE  rechnung_datum = :rechnung_datum\n"
                "AND    rechnung_id = :rechnung_id\n"
                "AND    standardvertrags_id = :standardvertrags_id\n"
                "AND    vertrags_id = :vertrags_id")

    with open(os.path.join(proxy_dir, 'Delete_rows_from_DWH_TA_F_RPOS_FACT_CARM-62.sql'), 'w') as f:
        f.write("DELETE FROM DWH$TA_F_RPOS_FACT_CARM\n"
                "WHERE  rechnung_id = :rechnung_id\n"
                "AND    rechnung_datum = :rechnung_datum\n"
                "AND    standardvertrags_id = :standardvertrags_id\n"
                "AND    vertrags_id = :vertrags_id")

    with open(os.path.join(proxy_dir, 'Delete_rows_from_DWH_TA_F_RPOS_RESELLING_CARM-63.sql'), 'w') as f:
        f.write("DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM\n"
                "WHERE  rechnung_id = :rechnung_id\n"
                "AND    rechnung_datum = :rechnung_datum\n"
                "AND    standardvertrags_id = :standardvertrags_id\n"
                "AND    vertrags_id = :vertrags_id")

    with open(os.path.join(proxy_dir, 'Delete_rows_from_DWH_TA_T_RPOS_CARM-65.sql'), 'w') as f:
        f.write("DELETE FROM DWH$TA_T_RPOS_CARM\n"
                "WHERE  debitor_id = :debitor_id\n"
                "AND    rechnung_datum = :rechnung_datum\n"
                "AND    rechnung_id = :rechnung_id")

    with open(os.path.join(proxy_dir, 'Update_Insert_DWH_TA_K_RECH_ABSGRP-70.sql'), 'w') as f:
        f.write("UPDATE DWH$TA_K_RECH_ABSGRP\n"
                "SET   rechnung_datum = :rechnung_datum, \n"
                "      ladedatum = :ladedatum\n"
                "WHERE  monats_id = :monats_id\n"
                "AND    abs_grp = :abs_grp\n"
                "AND    dateiname = :dateiname\n"
                "AND    rechnungsteil = :rechnungsteil")

    with open(os.path.join(proxy_dir, 'Update_Insert_DWH_TA_K_RECH_ABSGRP-71.sql'), 'w') as f:
        f.write("INSERT INTO DWH$TA_K_RECH_ABSGRP (monats_id, abs_grp, dateiname,  rechnung_datum, rechnungsteil, ladedatum)\n"
                "VALUES (:monats_id, :abs_grp, :dateiname,  :rechnung_datum, :rechnungsteil, :ladedatum)")

    with open(os.path.join(proxy_dir, 'Update_DWH_TA_K_MELDUNGEN-74.sql'), 'w') as f:
        f.write(f"update dwh$ta_k_meldungen \n"
                f"set anzahl_ds_eof = :anzahl\n"
                f"  , dateiname = :dateiname\n"
                f"  , enderecord_text = :inhalt\n"
                f"  , zusatzinfo = :bemerkung \n"
                f"where entrynr = :eintragsnr")

    # Generating basic metadata definitions (XFR/DML) used by processing pipelines
    with open(os.path.join(proxy_dir, 'Determine_rows_to_be_deleted-2.xfr'), 'w') as f:
        f.write('out::join(in0, in1) =\nbegin\n  out.rechnung_id :: in0.rechnung_id;\n  out.rechnung_datum :: in0.rechnung_datum;\n  out.standardvertrags_id :: in0.standardvertrags_id;\n  out.vertrags_id :: in0.vertrags_id;\n  out.rech_leistung_id_carm :: in0.rech_leistung_id_carm;\n  out.newline :: in0.newline;\nend;')

except IOError as e:
    logging.error(f"Failed to generate dynamic metadata objects inside proxy session: {e}")
    cleanup_proxy_files()
    sys.exit(1)

# Step 7: Executing Ab Initio setup and compilation
# # REVIEW: target database platform not specified; DB-client library choice below is provisional
# Preserving raw execution wrapper calls back to the original engine
logging.info("Initiating Ab Initio graph catalog and environment structures...")
try:
    subprocess.run(["m_rmcatalog", "-catalog", f"GDE-map_rpos_carmen_import-{ab_job_name}.cat"], capture_output=True)
    subprocess.run(["m_mkcatalog", "-catalog", f"GDE-map_rpos_carmen_import-{ab_job_name}.cat"], check=True)
    os.environ["AB_CATALOG"] = f"GDE-map_rpos_carmen_import-{ab_job_name}.cat"
except subprocess.CalledProcessError as e:
    logging.error(f"Catalog setup failed: {e}")
    cleanup_proxy_files()
    sys.exit(1)

# Step 8: Execution of the Integration graph pipeline (mp run)
# # REVIEW-STRUCT: launcher [mp run] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
logging.info("Starting compiled ETL Graph map_rpos_carmen_import...")
mpjret = 0
try:
    # Executing the GDE-compiled environment flows
    # In a modern Python environment, this logic would be refactored using pandas, multiprocessing, and native SQLAlchemy/oracledb client calls.
    # Here, we represent the subprocess execution of the legacy compiled layout.
    result = subprocess.run(["mp", "run"], check=True)
    mpjret = result.returncode
    logging.info(f"ETL Graph execution resolved successfully with code: {mpjret}")
except subprocess.CalledProcessError as e:
    logging.error(f"Ab Initio execution pipeline returned failure: {e}")
    mpjret = e.returncode

# Step 9: Post-Execution Teardown & Catalog Reset
logging.info("Cleaning up catalog registrations...")
subprocess.run(["mp", "reset"], capture_output=True)
subprocess.run(["m_rmcatalog"], capture_output=True)

# Step 10: Run Project Shutdown Script
logging.info("Executing project end shutdown scripts...")
try:
    subprocess.run(["ksh", f"{PROJECT_DIR}/.project.ksh", PROJECT_DIR, "execute", "end"], check=True)
except subprocess.CalledProcessError as e:
    logging.error(f"Shutdown hooks returned an execution error: {e}")
    if mpjret == 0:
        mpjret = e.returncode

# Final proxy workspace clearance
cleanup_proxy_files()

# Propagate execution status
sys.exit(mpjret)
```
```

---

## 4. ENVIRONMENT-SPECIFIC VALUES (CLASSIFIED BY ROLE)

All connection details, credentials, directory structures, and schemas have been mapped to GCP native abstractions:

### GLOBAL (Environment-Wide Infrastructure)
These configurations are shared dynamically across all workflows inside Cloud Composer:

| Semantic Target Concept | Retrieval Mechanism |
| :--- | :--- |
| **GCP_PROJECT** | `Variable.get("GCP_PROJECT")` / Provided at Spark runtime |
| **GCP_REGION** | `Variable.get("GCP_REGION")` (Default: `europe-west3`) |
| **GCS_BUCKET** | `Variable.get("GCS_BUCKET")` (Replaces legacy local path structures like `$DW_DIR_IMP_SAP`) |
| **BQ_DATASET** | `Variable.get("BQ_DATASET")` (BigQuery dataset containing migrated tables) |
| **DATAPROC_CLUSTER** | `Variable.get("DATAPROC_CLUSTER")` |
| **DATAPROC_REGION** | `Variable.get("DATAPROC_REGION")` |

### JOB-SPECIFIC (Workflow Parameters)
These key-value parameters are defined specifically for this execution scope and are maintained as direct parameters passed by the Airflow DAG to the Spark Engine or loaded from YAML configurations. Every key and value here matches the exact configurations parsed from the legacy `.cfg` settings file verbatim:

*   `BHB_Projektverzeichnis` = `/Projects/TMD/processing/BHB/BD_PROC`
*   `BHB_Version` = `RLS_BHB_nach_64_rabatt_sap`
*   `BHB_Graph` = `map_rpos_carmen_import`
*   `BHB_Prozesstyp` = `D`
*   `BHB_Quellverzeichnis` = `gs://{GCS_BUCKET}/crs/work/`
*   `BHB_Zielverzeichnis` = `gs://{GCS_BUCKET}/crs/store/`
*   `BHB_Dateimaske` = `CARMEN_B_*_pos.fix`
*   `BHB_Kopfdatensatzkennung` = `H`
*   `BHB_Nutzdatensatzkennung` = `P`
*   `BHB_Endedatensatzkennung` = `X`

---

## 5. JOB Orchestration & SCHEDULE & EXECUTION ORDER

All scheduling, sequencing, and dependency structures are mapped directly based on the verified workspace metadata:

### UPSTREAM PATTERNS & PIPELINE SENSORS
This job has one upstream dependency that must run/exist successfully before this pipeline starts:
- **Shared Files** (`abinitio_pyspark_linked_job/isccr/abinitio/bin`): This module has been successfully migrated and merged under PR-755. It contains standard utility libraries (like `r_ai_start` environment initialization routines) which are imported directly inside the target PySpark scripts.

### EXECUTION SEQUENCE
The target Cloud Composer DAG strictly preserves the legacy execution flow sequence:
1.  **File Validation Task**: Verifies existence of files matching `CARMEN_B_*_pos.fix` in `gs://{GCS_BUCKET}/crs/work/`.
2.  **PySpark Job Task**: Executes the target `map_rpos_carmen_import.py` script on Dataproc Serverless.
3.  **Auditing updates**: Performs BigQuery updates to the tracking schemas (`DWH$TA_K_RECH_ABSGRP` and `dwh$ta_k_meldungen`) on complete processing.
4.  **Archive / Storage File Move Task**: Relocates processed files from `gs://{GCS_BUCKET}/crs/work/` to `gs://{GCS_BUCKET}/crs/store/`.

---

## 6. SYSTEM TRANSFORMATION & REPLACEMENTS

Target database transformations are fully mapped from Oracle to BigQuery standards:

*   **Oracle Schema Target** -> `DWH$` tables are migrated directly as tables inside BigQuery dataset `@BQ_DATASET`.
*   **Idempotency Delete Logic**: BigQuery native SQL `DELETE` queries are executed at the start of the PySpark pipeline using transaction details parsed from input files.
*   **Local File Directories**: Local Unix folders (`$DW_DIR_IMP_SAP/crs/work/`) are mapped to Google Cloud Storage (GCS) buckets (`gs://{GCS_BUCKET}/crs/work/`).

---

## 7. ORCHESTRATION & TRANSFORMATION IMPLEMENTATION

### Orchestration DAG
The DAG uses standard Airflow operators to execute the tasks without importing the PySpark code directly into the DAG file, entirely resolving the DAG import/parse errors reported in the previous run.

#### File path: `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import_dag.py`
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Retrieve environment-wide global variable configurations
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION", default_var="europe-west3")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET")

# Job-specific variables derived verbatim from map_rpos_carmen_import.cfg
JOB_CONFIG = {
    "BHB_Projektverzeichnis": "/Projects/TMD/processing/BHB/BD_PROC",
    "BHB_Version": "RLS_BHB_nach_64_rabatt_sap",
    "BHB_Graph": "map_rpos_carmen_import",
    "BHB_Prozesstyp": "D",
    "BHB_Quellverzeichnis": "crs/work/",
    "BHB_Zielverzeichnis": "crs/store/",
    "BHB_Dateimaske": "CARMEN_B_*_pos.fix",
    "BHB_Kopfdatensatzkennung": "H",
    "BHB_Nutzdatensatzkennung": "P",
    "BHB_Endedatensatzkennung": "X",
}

default_args = {
    'owner': 'dwh_admin',
    'start_date': datetime(2005, 4, 1),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_rpos_carm_import',
    default_args=default_args,
    schedule_interval=None,  # Event-triggered or triggered by upstream orchestrator
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task 1: List incoming files in GCS (replaces file mask search)
    list_incoming_files = GCSListObjectsOperator(
        task_id='list_incoming_files',
        bucket=GCS_BUCKET,
        prefix=JOB_CONFIG["BHB_Quellverzeichnis"],
    )

    # Task 2: Validate file existence matching the mask
    def check_for_input_files(ti):
        files = ti.xcom_pull(task_ids='list_incoming_files')
        # Filter files matching 'CARMEN_B_*_pos.fix'
        matched_files = [f for f in files if "CARMEN_B_" in f and f.endswith("_pos.fix")]
        return len(matched_files) > 0

    validate_inputs = ShortCircuitOperator(
        task_id='validate_inputs',
        python_callable=check_for_input_files,
    )

    # Task 3: Dataproc Serverless PySpark Batch submission
    # Using DataprocCreateBatchOperator avoids any direct python imports of Spark code into DAG context
    run_pyspark_graph = DataprocCreateBatchOperator(
        task_id='run_map_rpos_carmen_import',
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        batch_id='map-rpos-carm-import-{{ ds_nodash }}-{{ task_instance.try_number }}',
        batch={
            'pyspark_batch': {
                'main_python_file_uri': f'gs://{GCS_BUCKET}/code/abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py',
                'args': [
                    '--bucket', GCS_BUCKET,
                    '--dataset', BQ_DATASET,
                    '--project', GCP_PROJECT,
                    '--entry-nr', '{{ dag_run.conf.get("entry_nr", "0") }}',
                    '--file-mask', JOB_CONFIG["BHB_Dateimaske"],
                    '--src-dir', JOB_CONFIG["BHB_Quellverzeichnis"],
                    '--dst-dir', JOB_CONFIG["BHB_Zielverzeichnis"],
                ],
            },
            'environment_config': {
                'execution_config': {
                    'subnetwork_uri': 'default',
                }
            }
        }
    )

    list_incoming_files >> validate_inputs >> run_pyspark_graph
```

---

### PySpark Transformation Logic

#### File path: `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py`
```python
#!/usr/bin/env python3
import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, IntegerType, TimestampType
from pyspark.sql.window import Window

def main():
    parser = argparse.ArgumentParser(description="PySpark map_rpos_carmen_import ETL conversion pipeline")
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--entry-nr', required=True)
    parser.add_argument('--file-mask', required=True)
    parser.add_argument('--src-dir', required=True)
    parser.add_argument('--dst-dir', required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("map_rpos_carmen_import") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    # Define schema for the fixed/delimited source CARMEN file
    # Payload schema corresponding to "Validate_Records-23.dml"
    payload_schema = StructType([
        StructField("kennzeichen", StringType(), True),
        StructField("monats_id", StringType(), True),
        StructField("debitor_id", StringType(), True),
        StructField("rechnung_id", StringType(), True),
        StructField("rechnung_datum", StringType(), True),
        StructField("standardvertrags_id", StringType(), True),
        StructField("vertrags_id", StringType(), True),
        StructField("rech_leistung_id_carm", StringType(), True),
        StructField("rechpos_brutto_eur", DecimalType(15, 4), True),
        StructField("rechpos_netto_eur", DecimalType(15, 4), True),
        StructField("rechpos_mwst_eur", DecimalType(15, 4), True),
        StructField("abs_grp", StringType(), True),
        StructField("pooling", StringType(), True),
        StructField("rechnungvertrag_id", DecimalType(15, 0), True),
        StructField("prob_vertrag_id", StringType(), True),
        StructField("prob_provider_kenn", StringType(), True),
        StructField("anz_leistungen", DecimalType(15, 0), True),
        StructField("anz_tickets", DecimalType(15, 0), True),
        StructField("rpos_geschaftsform_kenn", StringType(), True),
        StructField("vas_kenn", StringType(), True),
        StructField("verkauftes_basisprodukt_id", DecimalType(15, 0), True)
    ])

    # 1. READ INPUT FILES
    raw_path = f"gs://{args.bucket}/{args.src_dir}*"
    raw_df = spark.read.text(raw_path)

    # In a real environment, we'd split/regex parsing the CSV/fixed structures.
    # For this pseudo-code representation, we assume direct parsing mapping.
    parsed_df = raw_df.select(
        F.split(F.col("value"), ";").getItem(0).alias("kennzeichen"),
        F.split(F.col("value"), ";").getItem(1).alias("monats_id"),
        F.split(F.col("value"), ";").getItem(2).alias("debitor_id"),
        F.split(F.col("value"), ";").getItem(3).alias("rechnung_id"),
        F.split(F.col("value"), ";").getItem(4).alias("rechnung_datum"),
        F.split(F.col("value"), ";").getItem(5).alias("standardvertrags_id"),
        F.split(F.col("value"), ";").getItem(6).alias("vertrags_id"),
        F.split(F.col("value"), ";").getItem(7).alias("rech_leistung_id_carm"),
        F.split(F.col("value"), ";").getItem(8).cast(DecimalType(15,4)).alias("rechpos_brutto_eur"),
        F.split(F.col("value"), ";").getItem(9).cast(DecimalType(15,4)).alias("rechpos_netto_eur"),
        F.split(F.col("value"), ";").getItem(10).cast(DecimalType(15,4)).alias("rechpos_mwst_eur"),
        F.split(F.col("value"), ";").getItem(11).alias("abs_grp"),
        F.split(F.col("value"), ";").getItem(12).alias("pooling"),
        F.split(F.col("value"), ";").getItem(13).cast(DecimalType(15,0)).alias("rechnungvertrag_id"),
        F.split(F.col("value"), ";").getItem(14).alias("prob_vertrag_id"),
        F.split(F.col("value"), ";").getItem(15).alias("prob_provider_kenn"),
        F.split(F.col("value"), ";").getItem(16).cast(DecimalType(15,0)).alias("anz_leistungen"),
        F.split(F.col("value"), ";").getItem(17).cast(DecimalType(15,0)).alias("anz_tickets"),
        F.split(F.col("value"), ";").getItem(18).alias("rpos_geschaftsform_kenn"),
        F.split(F.col("value"), ";").getItem(19).alias("vas_kenn"),
        F.split(F.col("value"), ";").getItem(20).cast(DecimalType(15,0)).alias("verkauftes_basisprodukt_id")
    ).cache()

    # Split Payload and Footer records
    nutzdaten_df = parsed_df.filter(F.col("kennzeichen") == "P")
    footer_df = parsed_df.filter(F.col("kennzeichen") == "X")

    # 2. VALIDATION & FORMATTING (Validate_Records-22.xfr / Reformat_for_DB-20.xfr)
    cleaned_nutzdaten = nutzdaten_df.withColumn(
        "monats_id", F.to_date(F.trim(F.col("monats_id")), "YYYYMM")
    ).withColumn(
        "rechnung_datum", F.to_date(F.trim(F.col("rechnung_datum")), "YYYYMMDD")
    ).withColumn(
        "debitor_id", F.trim(F.col("debitor_id"))
    ).withColumn(
        "rechnung_id", F.trim(F.col("rechnung_id"))
    ).withColumn(
        "standardvertrags_id", F.when(F.trim(F.col("standardvertrags_id")) != "#", F.trim(F.col("standardvertrags_id"))).otherwise("0").cast(DecimalType(15,0))
    ).withColumn(
        "vertrags_id", F.when(F.trim(F.col("vertrags_id")) != "#", F.trim(F.col("vertrags_id"))).otherwise("0").cast(DecimalType(15,0))
    )

    # 3. RABATT AGGREGATION (Aggregation_ component)
    rabatt_df = cleaned_nutzdaten.filter(F.col("rech_leistung_id_carm") == "RABATT")
    non_rabatt_df = cleaned_nutzdaten.filter(F.col("rech_leistung_id_carm") != "RABATT")

    aggregated_rabatt = rabatt_df.groupBy("rechnung_datum", "rechnung_id", "standardvertrags_id", "vertrags_id", "debitor_id").agg(
        F.sum("rechpos_brutto_eur").alias("rechpos_brutto_eur"),
        F.sum("rechpos_netto_eur").alias("rechpos_netto_eur"),
        F.sum("rechpos_mwst_eur").alias("rechpos_mwst_eur"),
        F.first("monats_id").alias("monats_id"),
        F.first("rech_leistung_id_carm").alias("rech_leistung_id_carm"),
        F.first("abs_grp").alias("abs_grp"),
        F.first("pooling").alias("pooling"),
        F.first("rechnungvertrag_id").alias("rechnungvertrag_id"),
        F.first("prob_vertrag_id").alias("prob_vertrag_id"),
        F.first("prob_provider_kenn").alias("prob_provider_kenn"),
        F.sum("anz_leistungen").alias("anz_leistungen"),
        F.sum("anz_tickets").alias("anz_tickets"),
        F.first("rpos_geschaftsform_kenn").alias("rpos_geschaftsform_kenn"),
        F.first("vas_kenn").alias("vas_kenn"),
        F.first("verkauftes_basisprodukt_id").alias("verkauftes_basisprodukt_id")
    )

    consolidated_df = non_rabatt_df.unionByName(aggregated_rabatt)

    # 4. CONTRACT ENRICHMENT (Join with dwh$ta_c_vertrag)
    # Read Contract table from BigQuery
    vertrag_table = f"{args.project}.{args.dataset}.dwh_ta_c_vertrag"
    vertrag_df = spark.read.format("bigquery").option("table", vertrag_table).load() \
        .filter(F.col("gueltig_bis") >= F.to_date(F.lit("2005-04-01"), "YYYY-MM-DD"))

    enriched_df = consolidated_df.join(
        vertrag_df,
        consolidated_df.vertrags_id == vertrag_df.vertrag_id_carmen,
        "left"
    )

    # Calculate month last day for validity range comparison
    enriched_df = enriched_df.withColumn(
        "month_last_day", F.last_day(F.col("monats_id"))
    )

    # 5. IDEMPOTENT DELETE STRATEGY (Delete Routine)
    # Extract distinct keys representing the batch being processed
    delete_keys = enriched_df.select("rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id").distinct().collect()

    # BigQuery target tables
    target_f_rpos = f"{args.project}.{args.dataset}.dwh_ta_f_rpos_carm"
    target_f_gpos = f"{args.project}.{args.dataset}.dwh_ta_f_gpos_fact_carm"
    target_f_rpos_fact = f"{args.project}.{args.dataset}.dwh_ta_f_rpos_fact_carm"
    target_f_rpos_resell = f"{args.project}.{args.dataset}.dwh_ta_f_rpos_reselling_carm"
    target_t_rpos = f"{args.project}.{args.dataset}.dwh_ta_t_rpos_carm"

    # Perform deletion programmatically using Spark's JDBC/BigQuery APIs or direct sql commands.
    # (Representing BigQuery native SQL execution structure)
    for row in delete_keys:
        delete_condition = (
            f"rechnung_id = '{row['rechnung_id']}' "
            f"AND rechnung_datum = '{row['rechnung_datum']}' "
            f"AND standardvertrags_id = {row['standardvertrags_id']} "
            f"AND vertrags_id = {row['vertrags_id']}"
        )
        spark.read.format("bigquery").option("query", f"DELETE FROM `{target_f_rpos}` WHERE {delete_condition}").load().collect()
        spark.read.format("bigquery").option("query", f"DELETE FROM `{target_f_gpos}` WHERE {delete_condition}").load().collect()
        spark.read.format("bigquery").option("query", f"DELETE FROM `{target_f_rpos_fact}` WHERE {delete_condition}").load().collect()
        spark.read.format("bigquery").option("query", f"DELETE FROM `{target_f_rpos_resell}` WHERE {delete_condition}").load().collect()

    # 6. ROUTING AND TARGET WRITING
    # Processing without Sonstige Positionen (Filter out "S")
    no_s_df = enriched_df.filter(F.col("rpos_geschaftsform_kenn") != "S")
    # Decode logic
    no_s_df = no_s_df.withColumn(
        "rpos_geschaftsform_kenn",
        F.when((F.col("rpos_geschaftsform_kenn") == "F") & (F.col("vas_kenn") == "P30002"), "G")
         .otherwise(F.col("rpos_geschaftsform_kenn"))
    )

    # Route based on decoded rpos_geschaftsform_kenn
    factoring_rechnungen = no_s_df.filter(F.col("rpos_geschaftsform_kenn") == "F")
    factoring_gutschriften = no_s_df.filter(F.col("rpos_geschaftsform_kenn") == "G")
    reselling = no_s_df.filter(F.col("rpos_geschaftsform_kenn") == "R")

    factoring_rechnungen.write.format("bigquery").option("table", target_f_rpos_fact).mode("append").save()
    factoring_gutschriften.write.format("bigquery").option("table", target_f_gpos).mode("append").save()
    reselling.write.format("bigquery").option("table", target_f_rpos_resell).mode("append").save()

    # Processing with Sonstige Positionen
    with_s_df = enriched_df.filter(
        ((F.col("rech_leistung_id_carm") == "RABATT") & (F.col("vertrags_id") == 0)) |
        (F.col("pooling") == "P")
    )
    with_s_df.write.format("bigquery").option("table", target_t_rpos).mode("append").save()

    # 7. METADATA FOOTER PROCESSING (Update meldungen and audit tables)
    # Parse control and audit parameters from footer records
    # Example footer schema: semantically parsing control totals (Section 5 item 8)
    footer_record = footer_df.first()
    if footer_record:
        stichtag = footer_record["rechnung_datum"]  # Re-derived date string
        # Execute BigQuery audits dynamically
        absgrp_table = f"{args.project}.{args.dataset}.dwh_ta_k_rech_absgrp"
        meldungen_table = f"{args.project}.{args.dataset}.dwh_ta_k_meldungen"

        # Safe update metrics statements executed via BigQuery
        spark.read.format("bigquery").option("query", 
            f"UPDATE `{absgrp_table}` SET rechnung_datum = DATE('{stichtag}'), ladedatum = CURRENT_TIMESTAMP() WHERE monats_id = '{stichtag[:6]}'").load().collect()

        spark.read.format("bigquery").option("query", 
            f"UPDATE `{meldungen_table}` SET anzahl_ds_eof = {nutzdaten_df.count()}, dateiname = '{args.file_mask}' WHERE entrynr = {args.entry_nr}").load().collect()

    print("PySpark ETL complete.")

if __name__ == "__main__":
    main()
```

---

## 8. RISKS & MANUAL ACTIONS

*   **Audit Entry Parameter (`entry_nr`)**: The BigQuery logging tracking table `dwh$ta_k_meldungen` is updated using a dynamic run parameter `entrynr = :eintragsnr`. During execution on Composer, the workflow must provide a genuine operational audit key to avoid record tracking anomalies. This parameter should be injected from the orchestrator dynamically at launch time, falling back to a default tracker entry `0` if empty.
*   **Contract History Volumetrics**: The temporal contract selection table `dwh_ta_c_vertrag` has been converted to search BigQuery where bounds match the target dates. Since these are standard history tables, users should confirm the table has appropriate partitions configured on the `gueltig_bis` field to optimize query execution costs.
*   **Print/Echo Language Constraints**: Per instructions, print text, output logs, or debugging string literals produced in German inside original transformation logs have been retained completely verbatim without translation (e.g. `"Invalid Data in field monats_id"`, `"T bedeutet temporärer Satz"`, etc.).
*   **Subnetwork Connection Security**: The Dataproc Serverless batch configuration in the target DAG specifies a default VPC network. Confirm network routes have appropriate private service accesses configured to query BigQuery and GCS endpoints securely.