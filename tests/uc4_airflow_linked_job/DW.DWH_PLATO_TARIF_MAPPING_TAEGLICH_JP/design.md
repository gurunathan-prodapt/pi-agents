### SECTION 1 — DESIGN DOCUMENT (MCP OUTPUT VERBATIM)

#### 1. Overview
This UC4 workflow module defines a single Unix job task, `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`. It acts as an operational utility step (a placeholder or "dummy" task) within a larger daily Plato tariff mapping sequence. The script runs a non-operational dummy shell command execution ("Doing nothing"), returning a success state immediately. It is scheduled to execute as part of a daily batch execution sequence.

---

#### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `JOBS_UNIX` | `1` (Active) | Operational utility/dummy Unix job acting as a process synchronization or placeholder step in the Plato tariff workflow. |

---

#### 3. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_dummy_absd_plato_tarife` |
| **schedule** | `None` *(Note: This job is designed to be triggered within a parent workflow/JOBP. No independent cron schedule is specified in this single file export.)* |
| **start_date** | `datetime(2026, 3, 30)` |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation**| `False` *(Source UC4 active flag was `1`)* |
| **default_args** | `{'owner': 'DW.UNIX.ISTNS', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` |

---

#### 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dwh_dummy_absd_plato_tarife` | `DataprocSubmitJobOperator` | `dw_dwh_dummy_absd_plato_tarife.py` | Project, Region, Cluster, Bucket | `0` | N/A | None | `CaleOn="0"` (None) | No (`ActFlg="1"`) | None | Executes the dummy shell command mapped to a PySpark placeholder script. |

---

#### 5. Task Dependency Map
Since only one `JOBS_UNIX` object is defined in the provided input, the task dependency mapping represents a simple execution step without parent or child linkages:

`start >> dwh_dummy_absd_plato_tarife >> end`

---

#### 6. Parameter and Variable Mapping
| UC4 Parameter | Value / Source | Airflow Equivalent / Placeholder |
| :--- | :--- | :--- |
| **HostDst** | `|DWHDWH1P|HOST` | `YOUR_DATAPROC_CLUSTER_NAME` |
| **Login** | `DW.UNIX.ISTNS` | `owner` / `service_account` parameter |
| **Ert (Estimated Runtime)** | `11 seconds` | Mapped as metadata in the execution environment configuration. |
| **UC4 Object Name** | `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | **Sanitised DAG ID:** `dw_dwh_dummy_absd_plato_tarife` |

---

#### 7. Error Handling and Retry Strategy
- **Retry Logic:** The source UC4 XML defines no retry conditions (`MaxRetCode=0`, no restart conditions configured). Airflow retries are set to `0` by default.
- **Sync Object Analysis:** No Sync objects (`<SYNCREF>`) are present. Standard concurrency limits (`max_active_runs=1`) are applied.
- **Postcondition Analysis:** No postconditions or recovery actions are specified. Any failure results in a straightforward pipeline alert.

---

#### 8. Developer Notes
- **Placeholder Replacements:** The developer must replace all placeholder values prefixed with `YOUR_` (e.g., `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_CLUSTER_NAME`) with target infrastructure variables or Airflow Variables.
- **Independent Execution vs. Workflow Integration:** Since this export contains only a single `JOBS_UNIX` step without its enclosing parent Job Plan (`JOBP`), this DAG is configured to be triggered externally (`schedule=None`). If integrated back into a master sequence, target this DAG utilizing a `TriggerDagRunOperator`.
- **Target Script Path:** The executable is mapped to `gs://YOUR_BUCKET_NAME/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py` following standard naming convention policies.

---

### SECTION 2 — PSEUDOCODE

```python
── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.empty import EmptyOperator

── GCP Configuration ────────────────────────────────────
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
GCP_REGION = "YOUR_DATAPROC_REGION"
DP_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"

── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'DW.UNIX.ISTNS',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=default_args,
    description='Airflow migration of UC4 job DW.DWH_DUMMY_ABSD_PLATO_TARIFE',
    schedule=None,  # Triggered externally by parent JOBP pipeline
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False, # Source active flag was 1 (True)
)

── Tasks ────────────────────────────────────────────────
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Mapped from JOBS_UNIX: DW.DWH_DUMMY_ABSD_PLATO_TARIFE
dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
    task_id='dwh_dummy_absd_plato_tarife',
    project_id=GCP_PROJECT_ID,
    region=GCP_REGION,
    job={
        'reference': {
            'project_id': GCP_PROJECT_ID,
            'job_id': 'dw_dwh_dummy_absd_plato_tarife_{{ run_id | ts_nodash_lower }}'
        },
        'placement': {
            'cluster_name': DP_CLUSTER_NAME
        },
        'pyspark_job': {
            'main_python_file_uri': PYSPARK_SCRIPT_URI,
            'args': []  # Script body only outputs a print statement
        }
    },
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

── Dependencies ─────────────────────────────────────────
start >> dwh_dummy_absd_plato_tarife >> end
```

---

### SECTION 3 — ADD CONTEXT THE MCP COULD NOT SEE

#### 1. File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Migrates the UC4 JOB definition into an Airflow DAG structure. |

#### 2. Job Dependencies & Lineage
* **Upstream / Caller:**
  * `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` (Cross-job hand-off, owned by a different assembled job). In Cloud Composer/Airflow, this dummy job will be triggered as a task within the parent DAG mapping for `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`.
* **Downstream / Output Targets:**
  * `EXT:DWHDWH1P`: Mapped legacy HTTP/Host link target.
  * `PACKAGE:DW.UNIX.ISTNS`: Matches the legacy login/package context.

#### 3. Environment-Specific Values (Classification)
According to the **ENV VARIABLE POLICY**:

##### Global Environment Variables (Environment-wide constants):
* `GCP_PROJECT`: Sourced via `Variable.get("GCP_PROJECT")`.
* `GCP_REGION`: Sourced via `Variable.get("GCP_REGION")`.
* `GCS_BUCKET`: Sourced via `Variable.get("GCS_BUCKET")`.
* `DATAPROC_CLUSTER`: Sourced via `Variable.get("DATAPROC_CLUSTER")`.

These constants must resolve dynamically at runtime using Airflow Variables instead of being hardcoded inside the pipeline definitions.

##### Job-Specific Variables:
* `owner`: Set to `"DW.UNIX.ISTNS"` as retrieved from the `<Login>` parameter.

#### 4. Risks & Manual Actions
* **Verification of Target Folder Mirroring:** Ensure that `dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/` perfectly mirrors the source structure inside the Airflow DAG folder.
* **Pure Placeholder Optimization:** Because this job contains no functional logic other than executing a dummy script containing `:print Doing nothinig` (which must be preserved verbatim in any output execution logs or translated scripts under the **OUTPUT/PRINT LITERAL RULE**), a standard Airflow `BashOperator` running `echo "Doing nothinig"` or a Python-based equivalent can be substituted instead of spinning up a full Dataproc job if optimal resource management is desired.
* **Unmigrated Parent Pipeline Dependency:** The downstream wiring and execution triggers cannot be fully validated until the parent job DAG (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`) is migrated.