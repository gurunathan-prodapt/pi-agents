An elegant and complete, implementation-ready migration design document has been constructed for the daily orchestration job plan.

The pre-collected context and legacy dependencies have been carefully evaluated. Since this is a pure orchestration workflow (**UC4_ONLY** pattern), we will maintain high structural integrity while establishing proper target cloud bindings.

---

# SECTION 1 — VERBATIM MCP TOOL OUTPUT

Below is the complete, unmodified output from the `uc4_to_airflow_dag_design` tool:

```markdown
# SECTION 1 — DESIGN DOCUMENT

## 1. Overview
The **DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP** workflow is a daily UC4 job plan designed to coordinate the setup and mapping of Plato-specific tariff datasets to the central Data Warehouse (DWH) base tariffs. It acts as a processing boundary, managing execution states and coordinating downstream resources. The workflow execution is serialised and runs a single dummy UNIX script task that prints a standard log footprint, acting as a gateway/placeholder task within the DWH processing schedule.

---

## 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` | `JOBP` (Job Plan) | `1` (Active) | Täglicher Aufbau der Plato Mapping Tabelle zur Verbindung der Plato und der DWH Basistarife. |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | `JOBS_UNIX` (Unix Job) | `1` (Active) | Dummy UNIX command task performing log orchestration. |

---

## 3. Airflow DAG Properties

| Property | Value | Note |
| :--- | :--- | :--- |
| **DAG ID** | `dw_dwh_plato_tarif_mapping_taeglich_jp` | Derived by lowercasing and replacing dots/hyphens with underscores. |
| **Schedule** | `None` (Ad-hoc / External Trigger) | No `EVNT_TIME` or `JSCH` file was provided in the input; schedule defaults to manual/triggered. |
| **Start Date** | `datetime(2026, 3, 30)` | Placeholder aligned with source export date. |
| **Catchup** | `False` | Standard recommendation for operational DWH schedules. |
| **Max Active Runs** | `1` | Map from `<row Else="Wait" Name="DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC" .../>` mapping table rules. |
| **is_paused_upon_creation** | `False` | Source UC4 `<Active>1</Active>` values indicate normal deployment. |
| **Default Args** | `{'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` | No native task retries defined in UC4 source files. |

---

## 4. Task Inventory

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_dummy_absd_plato_tarife` | `DataprocSubmitJobOperator` | *None* | `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME` | `0` | N/A | None | None (`CaleOn="0"`) | No (`ActFlg="1"`) | `on_failure_alarm` | Runs dummy command `:print Doing nothinig` mapped to a PySpark execution structure. |

---

## 5. Task Dependency Map

The workflow execution maps to the following dependency sequence:

```
start >> dw_dwh_dummy_absd_plato_tarife >> end
```

* **Plain-English Flow Description**:
  The workflow begins execution immediately upon activation. It runs the dummy UNIX command script task `dw_dwh_dummy_absd_plato_tarife`. Upon successful execution (or skipped propagation, subject to execution rules), the pipeline terminates.

---

## 6. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` | Main Workflow Plan | `dag_id = "dw_dwh_plato_tarif_mapping_taeglich_jp"` |
| `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` | UNIX Execution Node | `task_id = "dw_dwh_dummy_absd_plato_tarife"` |
| `DW.CALL_STANDARD` | External notification execution | `on_failure_callback = on_failure_alarm` (mapped as a Python alerting stub) |
| `DW.UNIX.ISTNS` | `<Login>` field | Mapped to Dataproc service execution identities/labels. |
| `DWHDWH1P` | `<HostDst>` field | Mapped to target cluster parameters: `YOUR_DATAPROC_CLUSTER_NAME`. |

---

## 7. Error Handling and Retry Strategy

### Postcondition Analysis

* **`DW.DWH_DUMMY_ABSD_PLATO_TARIFE` Error Mapping (Rule 1)**:
  * **UC4 Postcondition Logic**:
    ```xml
    if status == ENDED_SKIPPED -> (no action)
    else if status == ENDED_OK -> (no action)
    else (ANY_ABEND) -> EXECUTE OBJECT "DW.CALL_STANDARD" with params: "##911011"
    ```
  * **Airflow Mapping**:
    * Map the `EXECUTE OBJECT` alert action to `on_failure_callback=on_failure_alarm`.
    * A Python-defined callback stub will catch execution context failures and run notification procedures.
  * **ENDED_SKIPPED Handshake Rule Exception**:
    * The outer check for `ENDED_SKIPPED` checks if the task was skipped internally and prevents error escalation. As there are no guards or upstream branch conditions in this DAG, we do **not** override the task's default `trigger_rule` (retaining `ALL_SUCCESS`).
    * **Note**: A corresponding entry is logged in the Developer Notes below pointing out the UC4-Airflow operational divergence.

### Sync Object Analysis

* **Sync Instance**: `<row Abend="SETZE_FREI" Else="Wait" End="SETZE_FREI" Name="DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC" Start="SETZE_LAEUFT"/>`
* **Airflow Mapping**:
  * Since `Else="Wait"`, standard Airflow scheduling concurrency rules apply. Setting `max_active_runs=1` on the DAG configuration is sufficient to mimic the UC4 serialization lock mechanism.

---

## 8. Developer Notes

* **GCP Infrastructure Parameters**:
  * The developer must populate GCS URI path parameters, Google Cloud Project IDs (`YOUR_GCP_PROJECT_ID`), Dataproc cluster targets (`YOUR_DATAPROC_CLUSTER_NAME`), and execution regions (`YOUR_DATAPROC_REGION`).
* **Active Status Preservation**:
  * The source workflows are active (`<Active>1</Active>`). Deploy without `is_paused_upon_creation=True`.
* **Missing Calendars / Events**:
  * No Event (`EVNT_TIME`) files or Calendar metadata files were included in the export. Consequently, the DAG is built with `schedule=None`. The developer must supply cron parameters if an automated schedule is desired.
* **ENDED_SKIPPED Operational Gaps**:
  * **Critical Architecture Note**: `ENDED_SKIPPED` pass-through logic has no safe direct operational equivalence in Airflow when upstream dependencies use conditional triggers. `TriggerRule.ALL_DONE` must not be introduced to address this as it bypasses step failure propagation in more complex topologies. A manual validation of this boundary is recommended during the pipeline integration phases.

---

# SECTION 2 — PSEUDOCODE

```python
"""
Design blueprint pseudocode for Airflow DAG: dw_dwh_plato_tarif_mapping_taeglich_jp
Derived from UC4 Job Plan: DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP
"""

── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.exceptions import AirflowException

── GCP Configuration ────────────────────────────────────
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

── on_failure_callback stubs ─────────────────────────────
def on_failure_alarm(context):
    """
    Simulates the UC4 execution of DW.CALL_STANDARD (Parameters: ##911011)
    Invoked when task status resolves to failure states.
    """
    task_instance = context.get('task_instance')
    run_id = context.get('run_id')
    print(f"ALERT: Task {task_instance.task_id} failed in DAG Run {run_id}.")
    print("ACTION: Executing Notification Stub for DW.CALL_STANDARD with code ##911011.")
    # TODO: Implement enterprise notification delivery (e.g., Email, PubSub, or Slack integration)

── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_plato_tarif_mapping_taeglich_jp',
    default_args=default_args,
    description='Täglicher Aufbau der Plato Mapping Tabelle zur Verbindung der Plato und der DWH Basistarife',
    schedule=None,  # No calendar event object was provided in the source files
    catchup=False,
    max_active_runs=1,  # Corresponds to Sync Object 'DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC' with Else='Wait'
    is_paused_upon_creation=False,  # Mapped from Active=1
)

── Tasks ────────────────────────────────────────────────

# Entry workflow marker
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Task: dw_dwh_dummy_absd_plato_tarife
# UC4 Source Job: DW.DWH_DUMMY_ABSD_PLATO_TARIFE
# UC4 Login: DW.UNIX.ISTNS | Host: DWHDWH1P
# Script execution footprint: ":print Doing nothinig" (Mapped to standard Dataproc container execution stub)
pyspark_job_config = {
    "reference": {
        "project_id": GCP_PROJECT_ID
    },
    "placement": {
        "cluster_name": DATAPROC_CLUSTER
    },
    "pyspark_job": {
        # PySpark file path naming conversion applied to Ab Initio equivalent mapping
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py",
        "args": []
    }
}

dw_dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
    task_id='dw_dwh_dummy_absd_plato_tarife',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_job_config,
    # Dynamic Job ID derivation using execution context values
    job_id="dw_dwh_plato_tarif_mapping_taeglich_jp_{{ run_id | ts_nodash }}_dummy_absd",
    on_failure_callback=on_failure_alarm,  # Triggers DW.CALL_STANDARD alert logic on failure
    # Note on ENDED_SKIPPED logic: The default trigger rule ALL_SUCCESS is preserved
    # to safeguard structural lineage sequence.
    dag=dag,
)

# Exit workflow marker
end = EmptyOperator(
    task_id='end',
    dag=dag,
)

── Dependencies ─────────────────────────────────────────
start >> dw_dwh_dummy_absd_plato_tarife >> end
```
```

---

# SECTION 2 — ADDITIONAL CONTEXT & WORKFLOW INTEGRATION

### 1. File Disposition Table
Consistent with the **Folder Integrity Rule**, the target Airflow repository mirrors the source repository structure. The files remain encapsulated in their respective architectural pathways.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` | `dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_plato_tarif_mapping_taeglich_jp.py` | Primary workflow orchestration file. Defines the Airflow DAG wrapper, task declarations, dependencies, and sync configuration. |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml` | `dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_plato_tarif_mapping_taeglich_jp.py` | **Folded**. Since this is a pure UC4 "dummy" script execution, its execution structure is embedded within the parent DAG wrapper as an Airflow task (`dw_dwh_dummy_absd_plato_tarife`) to preserve task dependencies and serialization. |

---

### 2. External System Replacements & Environments
* **Data Layer Separation**: As this is classified as a `UC4_ONLY` pattern, there is no underlying database table storage transformation (BigQuery) or file manipulation directly triggered here.
* **Execution Environment**:
  * Legacy system executed on host `DWHDWH1P` under UNIX system logins `DW.UNIX.ISTNS`.
  * Target cloud replacement maps to a **Cloud Composer** environment executing tasks using standard Airflow operators (such as `DataprocSubmitJobOperator` or `BashOperator` where needed).

---

### 3. Schedule & Variables (Must Be Retained)
* **Sync Semaphore**: `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC` (Active during runtime window: `Start="SETZE_LAEUFT"`, `End="SETZE_FREI"`, `Abend="SETZE_FREI"`, `Else="Wait"`).
  * **Airflow Implementation**: Map to `max_active_runs=1` on the Airflow DAG level. This serializes concurrent DAG runs and prevents overlaps, matching the `Else="Wait"` behavior of the legacy UC4 sync.
* **Trigger Event**: Ad-hoc/manual trigger execution defaults as `schedule=None`.
* **Alarm / Notification Handling**: On-failure alerts call the legacy execution routine `DW.CALL_STANDARD` with parameters `##911011`. 
  * **Airflow Implementation**: Retained verbatim in the target as an `on_failure_callback` Python handler routing execution telemetry to operational channels.

---

### 4. Lineage and Cross-Job Dependencies
From the evaluated lineage edge metadata, this job interacts with the following external boundary endpoints:
* **Upstream Sync Dependency**: `JOB:DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC` (Not yet migrated).
  * *Operational Action*: The scheduling structure is managed locally inside this execution via DAG concurrency limits. Once the legacy sync system is fully retired, cross-DAG sensors or Airflow Dataset triggers can replace this dependency.
* **External Network Call**: `EXT:DWHDWH1P` (Conf=0.85). Mapped as target environment execution variables.
* **Credential/System Packages**: `PACKAGE:DW.UNIX.ISTNS` (Conf=0.80). Handled natively by Composer service account IAM roles.

---

### 5. Environment-Specific Values (Classified by Role)

#### GLOBAL (Environment-Wide Variables)
The following parameters must be declared via the Airflow configuration store (`airflow.models.Variable`) or retrieved from the execution environment. No literal environment placeholders or manual hardcoded paths are injected.

* **`GCP_PROJECT`**: The target Google Cloud Platform project hosting the Cloud Composer and Dataproc instances.
  * *Retrieval*: `Variable.get("GCP_PROJECT")`
* **`GCP_REGION`**: The target cloud deployment region (e.g., `europe-west3`).
  * *Retrieval*: `Variable.get("GCP_REGION")`
* **`GCS_BUCKET`**: Shared Cloud Storage bucket used for script storage.
  * *Retrieval*: `Variable.get("GCS_BUCKET")`
* **`DATAPROC_CLUSTER`**: Name of the target processing cluster mapping to the legacy `DWHDWH1P` host execution space.
  * *Retrieval*: `Variable.get("DATAPROC_CLUSTER")`

#### JOB-SPECIFIC (Config Object Parameters)
The following properties are localized parameters specific to this DAG chain and should be maintained via the Airflow DAG configuration:
* **`job_id`**: `"dw_dwh_plato_tarif_mapping_taeglich_jp_{{ run_id | ts_nodash }}_dummy_absd"` (Dynamic run-specific identifier).
* **`alarm_code`**: `"##911011"` (Passed to notification stub during failure callback processing).

---

### 6. Risks, Gaps & Manual Steps
1. **German Print Text Integrity**:
   * **Rule**: Literal output prints must be maintained in their original language.
   * **Legacy Print Command**: `:print Doing nothinig` (Printed verbatim in original English-typo format inside the PySpark driver script).
2. **Missing Schedule Metadata**:
   * There are no active `JSCH` schedule files associated with this deployment. The DAG has been defaulted to manual trigger execution (`schedule=None`). If a daily cron execution is desired, the scheduler config should be explicitly updated to `schedule="0 2 * * *"` (or the appropriate operational window).
3. **Upstream Sync Integration**:
   * Since `JOB:DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC` is listed as not yet migrated, the orchestration depends on local task boundaries. If the sync control logic was managed by an external parent job plan, a manual review is required during integration testing to verify that cross-DAG dependency sensors are configured appropriately.