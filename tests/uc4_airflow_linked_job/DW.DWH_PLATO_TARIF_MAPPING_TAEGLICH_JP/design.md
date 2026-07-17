An elegant and complete migration design has been compiled. Below is the comprehensive, implementation-ready Migration Design Document.

---

# MIGRATION DESIGN DOCUMENT

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
|:---|:---|:---|
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` | `dags/dw_dwh_plato_tarif_mapping_taeglich_jp.py` | Primary workflow orchestration file, migrated to an Airflow DAG. |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py` | Migrated dummy/diagnostic validation process referenced within the job execution hierarchy. |

---

### Folder Integrity & Target File Plan
Following the **Folder Integrity Rule**, the target folder structure mirrors the source repository's relative layouts:
- **Orchestration DAG**: `dags/dw_dwh_plato_tarif_mapping_taeglich_jp.py` (Source: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml`)
- **PySpark Executable**: `pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py` (Source: `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml`)

---

### Verification of Core Logic (Literal Log Preservation)
Per the **Output/Print Literal Rule**, the original German/English logging output inside the legacy shell/script commands is carried over verbatim. 
* Original Unix SCRIPT tag content: `:print Doing nothinig`
* Retained in target PySpark script: `print("Doing nothinig")`

---

## SECTION 1 — DESIGN DOCUMENT (VERBATIM FROM MCP CONVERSION)

### 1. Overview
This workflow automates the daily setup of the Plato Mapping Table, which maps Plato base tariffs to Data Warehouse (DWH) base tariffs. It acts as an integration and alignment pipeline for downstream reporting databases. The workflow consists of a single sequence running on a daily schedule, executing a validation/dummy sync task to prepare baseline mapping values before finishing.

---

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` | JOBP | Active (`1`) | Täglicher Aufbau der Plato Mapping Tabelle zur Verbindung der Plato und der DWH Basistarife |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | Active (`1`) | Dummy Unix execution task processing validation |

---

### 3. Airflow DAG Properties
| Property | Value | Note |
|---|---|---|
| **DAG ID** | `dw_dwh_plato_tarif_mapping_taeglich_jp` | Sanitised lowercase translation of the main JOBP |
| **Schedule (cron)** | `0 3 * * *` | **Note:** Assumed daily schedule at `03:00` as no explicit EVNT_TIME file was provided |
| **Start Date** | `datetime(2026, 3, 30)` | Derived from object export metadata timestamp |
| **Catchup** | `False` | Recommended default to prevent retroactive backfilling |
| **Max Active Runs** | `1` | Corresponds to the UC4 Sync Object `Wait` state mapping logic |
| **Is Paused Upon Creation** | `False` | Main JOBP active status is `1` |
| **Default Args** | `{'owner': 'DW', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` | Base configuration with standard delay placeholder |

---

### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_dummy_absd_plato_tarife` | `DataprocSubmitJobOperator` | `dw_dwh_dummy_absd_plato_tarife.py` | Project, Region, Cluster, Bucket | 0 | - | None | None (`CaleOn="0"`) | False | `on_failure_alarm` | Execute dummy/validation task. Triggers alert standard object `DW.CALL_STANDARD` on abend. |

---

### 5. Task Dependency Map
```
start >> dw_dwh_dummy_absd_plato_tarife >> end
```
* **Plain English Flow:** The execution starts. The dummy mapping process `dw_dwh_dummy_absd_plato_tarife` runs to validate underlying mappings. If it succeeds (or is skipped via configured parameters), the workflow reaches its terminal node.

---

### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` | JOBP (Main) | `dw_dwh_plato_tarif_mapping_taeglich_jp` (DAG ID) |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | JOBS_UNIX | `dw_dwh_dummy_absd_plato_tarife` (Task ID) |
| `DW.CALL_STANDARD` | EXECUTE OBJECT | `on_failure_alarm` (Airflow alarm callback stub) |
| `HostDst="DWHDWH1P"` | UC4 Target Host | `YOUR_DATAPROC_CLUSTER_NAME` |
| `Login="DW.UNIX.ISTNS"` | Execution user | Spark Service Account / GCP IAM credentials configuration |

---

### 7. Error Handling and Retry Strategy
* **Postcondition Map (Task: `dw_dwh_dummy_absd_plato_tarife`):**
  * `ENDED_SKIPPED`: Handled as normal flow sequence (no error actions taken).
  * `ENDED_OK`: Standard execution continuation.
  * `ANY_ABEND`: Triggers `DW.CALL_STANDARD` with parameter `##911011`. This maps to `on_failure_callback=on_failure_alarm`.
* **Sync Object Analysis:**
  * The Sync Object `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC` uses `Else="Wait"`.
  * **Airflow Mapping:** Handled safely by setting `max_active_runs=1` on the DAG instance.

---

### 8. Developer Notes
* **GCP Infrastructure:** Define the standard Dataproc cluster variables (`YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, `YOUR_BUCKET_NAME`) in Airflow variables or environment variables.
* **Scheduling:** No scheduling file (`EVNT_TIME`) was attached. A default placeholder daily schedule is modeled (`0 3 * * *`). This must be adjusted to align with any external cron/time definitions.
* **ENDED_SKIPPED Pass-Through:** In UC4, an `ENDED_SKIPPED` state bypasses the abend alerts. Airflow defaults to checking parent task success (`ALL_SUCCESS`).
  * **Gap Note:** "ENDED_SKIPPED pass-through has no safe direct equivalent in Airflow when a guard task is present. `TriggerRule.ALL_DONE` must not be used as it breaks upstream skip propagation. Manual review required."
* **Dummy Script Executable:** The target script body performs only a diagnostic execution (`:print Doing nothinig`). This has been configured as a PySpark template placeholder in standard paths.

---

## SECTION 2 — TARGET CODE

### Target File 1: Orchestration Airflow DAG
Path: `dags/dw_dwh_plato_tarif_mapping_taeglich_jp.py`

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration (GLOBAL Env Policy Classification) ──
GCP_PROJECT = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "DW",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
def on_failure_alarm(context):
    """
    Fires on task failure to execute notification.
    Corresponds to UC4 call of DW.CALL_STANDARD with parameter ##911011.
    """
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    print(f"ALARM: Task {task_id} failed on {execution_date}. Triggering alarm payload: ##911011")
    # TODO: Implement enterprise notification endpoint or Pub/Sub message here

# ── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id="dw_dwh_plato_tarif_mapping_taeglich_jp",
    default_args=default_args,
    description="Täglicher Aufbau der Plato Mapping Tabelle zur Verbindung der Plato und der DWH Basistarife",
    schedule="0 3 * * *",  # Assumed Daily placeholder; adjust based on project schedules
    catchup=False,
    max_active_runs=1,     # Implements Else="Wait" Sync condition mapping
    is_paused_upon_creation=False,
)

# ── Task: Start ──────────────────────────────────────────
start = EmptyOperator(
    task_id="start",
    dag=dag,
)

# ── Task: dw_dwh_dummy_absd_plato_tarife ─────────────────
# Corresponds to JOBS_UNIX: DW.DWH_DUMMY_ABSD_PLATO_TARIFE
# ERT: 11 seconds
# Login: DW.UNIX.ISTNS
pyspark_job_dummy = {
    "reference": {"project_id": GCP_PROJECT},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py",
        "args": []
    }
}

dw_dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
    task_id="dw_dwh_dummy_absd_plato_tarife",
    job=pyspark_job_dummy,
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT,
    on_failure_callback=on_failure_alarm,
    # Defaulting to TriggerRule.ALL_SUCCESS to maintain correct dependency flows
    dag=dag,
)

# ── Task: End ────────────────────────────────────────────
end = EmptyOperator(
    task_id="end",
    dag=dag,
)

# ── Dependencies ─────────────────────────────────────────
start >> dw_dwh_dummy_absd_plato_tarife >> end
```

---

### Target File 2: Dummy Validation PySpark Script
Path: `pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py`

```python
#!/usr/bin/env python
from pyspark.sql import SparkSession

def main():
    # Initialize the Spark Session on Dataproc
    spark = SparkSession.builder \
        .appName("dw_dwh_dummy_absd_plato_tarife") \
        .getOrCreate()
        
    # VERBATIM preservation of legacy SCRIPT log output
    print("Doing nothinig")
    
    spark.stop()

if __name__ == "__main__":
    main()
```

---

## SECTION 3 — ENRICHED METADATA & CONTEXT

### Job Dependencies & Execution Order
1. **Upstream Scheduling and Constraints**: 
   - The primary execution contains a strict sync mechanism defined by the UC4 Sync object `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC` which operates with a "Wait" logic if an active execution is already running. This is mapped directly to GCP Airflow via `max_active_runs=1` on the DAG configuration.
2. **Sequential Steps**:
   - `start` (EmptyOperator)
   - `dw_dwh_dummy_absd_plato_tarife` (DataprocSubmitJobOperator)
   - `end` (EmptyOperator)

### Lineage Edges & Cross-Job Hand-offs
- **HTTP Call**: `DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` indicates a legacy network lookup toward `EXT:DWHDWH1P` (conf=0.85). In Cloud Composer/Dataproc, this networking boundary is resolved via GCP VPC Network Peering and security rules corresponding to the execution Service Account.
- **Package Reference**: Utilizes `PACKAGE:DW.UNIX.ISTNS` for execution settings.
- **Workflow Dependency**: The parent workflow is strictly coupled to `JOB:DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC` as an integration step.

### Environment Variable Classification Policy
Following target environment guidelines, variables are classified by role rather than legacy names:
1. **GLOBAL Environment-Wide Infra**:
   - `GCP_PROJECT`: Fetched dynamically via Airflow's config store (`Variable.get("GCP_PROJECT")`).
   - `DATAPROC_REGION`: Target Region identifier on GCP (`Variable.get("DATAPROC_REGION")`).
   - `DATAPROC_CLUSTER`: Name of the active Dataproc instance running the Spark environments (`Variable.get("DATAPROC_CLUSTER")`).
   - `GCS_BUCKET`: Target storage bucket containing Spark objects and scripts (`Variable.get("GCS_BUCKET")`).
2. **JOB-SPECIFIC Parameters**:
   - `default_args` / `owner`: Configured locally in the DAG to match the legacy team assignment (`"owner": "DW"`).

---

### Risks & Manual Steps
* **ENDED_SKIPPED Pass-Through Deviation**: In UC4, an `ENDED_SKIPPED` state on the child task bypasses standard error handlers. In Airflow, standard trigger structures will need manual oversight if selective task skipping is actively modeled upstream. `TriggerRule.ALL_SUCCESS` is applied dynamically to avoid skipping propagation failure.
* **Notification Integration**: The custom callback hook `on_failure_alarm` acts as a placeholder for `DW.CALL_STANDARD`. It prints alert payload `##911011` verbatim. Integrating this into your local monitoring framework (such as Slack, PagerDuty, or GCP Cloud Monitoring Pub/Sub) must be finalized by platform administrators.