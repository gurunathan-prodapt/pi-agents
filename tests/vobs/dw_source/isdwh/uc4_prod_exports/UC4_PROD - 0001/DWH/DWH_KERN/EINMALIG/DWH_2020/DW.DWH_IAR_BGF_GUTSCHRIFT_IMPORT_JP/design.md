=== OBJECT: DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT (EVNT_FILE) ===
active=1
title=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# DESIGN DOCUMENT: UC4 TO APACHE AIRFLOW MIGRATION

## 1. Overview
The UC4 object `DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT` is a File Event (`EVNT_FILE`) designed to monitor the arrival or existence of a specific data file related to "BGF Gutschrift Import" (Credit Memo/Refund Import). In UC4, File Events poll the filesystem and execute processing scripts or activate downstream workflows (JOBPs) once the target file is detected. Because the internal event script, file path parameters, and target actions were not supplied in this extraction, this migration design sets up a sensor-based DAG skeleton that must be filled in with the exact file path and downstream DAG trigger execution logic.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT` | EVNT_FILE | Active (1) | File Event for BGF Gutschrift Import |

## 3. Scheduling
* **Schedule Analysis**: No `EVNT_TIME` or `JSCH` object is present in this bundle. This File Event acts as an autonomous file-polling daemon in UC4.
* **Airflow Implementation**: In Airflow, file events are typically run on a continuous polling schedule or a regular interval (e.g., daily, hourly) to check for file arrivals. Without explicit schedule metadata, we default to `schedule=None` (triggered externally or on-demand) to avoid inventing execution intervals.
* **DAG Properties Setting**: `schedule=None`

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_run_iar_bgf_gutschrift_import_jp_evt` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Active=1 in UC4) |
| **default_args** | `{"retries": 1, "retry_delay": timedelta(minutes=5), "owner": "airflow"}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `sense_import_file` | `DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT` | `GCSObjectExistenceSensor` | `gs://YOUR_BUCKET_NAME/landing/UNCERTAIN_file_path_placeholder` | `bucket`, `object`, `poke_interval=300`, `timeout=86400` | 0 | N/A | None | None | N/A | None | #REVIEW-STRUCT: File path and storage system not provided. Assumed Google Cloud Storage (GCS) sensor as target environment pattern. |
| `trigger_downstream_workflow` | N/A | `TriggerDagRunOperator` | `UNCERTAIN_downstream_dag_id` | `trigger_dag_id`, `wait_for_completion=False` | 0 | N/A | None | None | True | None | #REVIEW-STRUCT: Downstream target workflow was not supplied in the extraction bundle. Stub task created to trigger processing once the target is identified. |

## 6. Task Dependency Map
```python
sense_import_file >> trigger_downstream_workflow
```

## 7. Sync / Concurrency Analysis
* No UC4 Sync (`SYNC`) or concurrency rules were provided in the extraction bundle.
* Setting `max_active_runs=1` at the DAG level prevents concurrent processing of multiple file-arrival events if a previous run is still waiting or processing.

## 8. Error Handling and Retry Strategy
* The default Airflow task retry strategy is applied (1 retry, 5-minute delay).
* Standard `TriggerRule.ALL_SUCCESS` is maintained.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| File Path (Missing) | Expected file landing path | `gcs_bucket` / `gcs_object` parameters or Airflow Variable |
| Trigger Target (Missing) | Expected downstream processing JOBP | `trigger_dag_id` in `TriggerDagRunOperator` |

## 10. Developer Notes
* **UNCERTAIN: File Path & Source System**: The actual file path, landing zone directory, or filesystem details were not part of the extraction. We have assumed GCS (`GCSObjectExistenceSensor`). If the file is on a local filesystem, SFTP server, or S3, replace the operator with `FileSensor`, `SFTPSensor`, or `S3KeySensor` respectively. `#REVIEW-STRUCT:`
* **UNCERTAIN: Downstream Trigger Action**: In UC4, File Events perform actions (such as activating a downstream JOBP) when a file is found. The downstream target is missing from this extraction. A `TriggerDagRunOperator` stub (`trigger_downstream_workflow`) has been placed in the DAG. Replace `UNCERTAIN_downstream_dag_id` with the actual sanitized DAG ID of the target ingestion workflow once identified. `#REVIEW-STRUCT:`

---

# PSEUDOCODE OUTLINE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ── GCP Configuration ────────────────────────────────────
# REVIEW-STRUCT: Define the landing bucket and folder path for the incoming Gutschrift file.
GCS_BUCKET = "YOUR_BUCKET_NAME"
# UNCERTAIN: File pattern and exact name is unknown. Placeholder below must be updated.
GCS_OBJECT_PATH = "landing/UNCERTAIN_file_path_placeholder"

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_run_iar_bgf_gutschrift_import_jp_evt",
    default_args=DEFAULT_ARGS,
    description="File Event Sensor DAG for BGF Gutschrift Import",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Externally triggered or manually scheduled
    catchup=False,
    max_active_runs=1,
    tags=["uc4_migration", "file_event", "bgf"],
) as dag:

    # ── Task: sense_import_file ──────────────────────────
    # Monitors GCS for the arrival of the Gutschrift import file
    sense_import_file = GCSObjectExistenceSensor(
        task_id="sense_import_file",
        bucket=GCS_BUCKET,
        object=GCS_OBJECT_PATH,
        poke_interval=300,       # Polls every 5 minutes
        timeout=86400,           # Fails after 24 hours of waiting
        mode="poke",
    )

    # ── Task: trigger_downstream_workflow ────────────────
    # REVIEW-STRUCT: Activates the downstream processing DAG.
    # UNCERTAIN_downstream_dag_id must be replaced with the actual processing workflow DAG ID.
    trigger_downstream_workflow = TriggerDagRunOperator(
        task_id="trigger_downstream_workflow",
        trigger_dag_id="UNCERTAIN_downstream_dag_id",
        wait_for_completion=False,  # Fire-and-forget down-stream run
        reset_dag_run=True,
        check_existence=True,
    )

    # ── Dependencies ─────────────────────────────────────────
    sense_import_file >> trigger_downstream_workflow
```

### Migration Design Addendum: DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT

This document provides the necessary execution context, schedule mappings, lineage connections, and environmental variables that the primary conversion tool could not see. 

---

### File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/EINMALIG/DWH_2020/DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP/DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT.xml` | `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/EINMALIG/DWH_2020/DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP/DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT.py` | Migrate the UC4 File Event polling mechanism and conditional script logic to an Airflow DAG structure preserving folder integrity. |

---

### Scheduling

- **Trigger Type**: Event-triggered File Event.
- **Legacy Behavior**: Polling interval of 30 minutes (`<TimePeriodTT>0030</TimePeriodTT>`) to verify if the file count is greater than 0 (`<Operator>G\|&gt;</Operator>` and `<Value>0</Value>`).
- **Airflow Scheduling**: The migrated DAG will be scheduled to run every 30 minutes using the cron schedule `*/30 * * * *` to check for file existence via sensors.

---

### Schedule & Variables

The following variables must be retained and mapped into the Airflow DAG's runtime context:

- **Executor**: `'DWHDWH1P'`  
  - *Airflow Implementation*: Mapped as a GLOBAL environment variable or configuration option to determine connection target host details.
- **StartJp**: `'DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP'`  
  - *Airflow Implementation*: Mapped as a JOB-SPECIFIC variable to represent the target DAG to trigger (`dw_dwh_iar_bgf_gutschrift_import_jp`).
- **date**: `SYS_DATE('JJJJMMTT')`  
  - *Airflow Implementation*: Resolved dynamically using Jinja context: `{{ ds_nodash }}` or via Python `datetime.utcnow().strftime('%Y%m%d')`.
- **AKTOBJ**: `'ACTIVATE_UC_OBJECT(JOBP,&StartJp)'`  
  - *Airflow Implementation*: Handled within Airflow using a `TriggerDagRunOperator` task that activates the downstream DAG.
- **CallOP**: `'ACTIVATE_UC_OBJECT(CALL,DW.CALL_STANDARD)'`  
  - *Airflow Implementation*: Mapped to a GLOBAL alert/notification utility (such as an `EmailOperator` or standard fallback notification function) to notify operators if downstream trigger activation fails.

---

### Lineage

- **Upstream Producer**: External file system or automated process delivering files matching `/app_dwh/sftp_users/istcomis/daten/tcom/iar/work/DWHK_DWHM_IAR_GUTSCHR_*.chk`.
- **Downstream Consumer**: `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP` (job: `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP`). This is a cross-job hand-off to be triggered upon successful file detection.

---

### Cross-File Dependencies

- **Included Modules**: The script references `: inc DW.HOLE_PFAD` which is a UC4 include utility containing standard path-resolution parameters. These paths must be mapped to target environment configurations.

---

### Target File Plan

- **Target File Path**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/EINMALIG/DWH_2020/DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP/DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT.py`
- **Language**: Python (Apache Airflow DAG)
- **Source File**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/EINMALIG/DWH_2020/DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP/DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT.xml`
- **Design Outline**:
  - Implements an `SFTPSensor` or `GCSObjectsWithPrefixPatternSensor` to match the wildcard file pattern `DWHK_DWHM_IAR_GUTSCHR_*.chk` at the defined file path.
  - Employs a custom `PythonOperator` to query the active states of the target DAG (checking if `dw_dwh_iar_bgf_gutschrift_import_jp` is already running).
  - Employs a `TriggerDagRunOperator` to start the target processing DAG if it is not currently active, with an `on_failure_callback` triggering the standard alert framework.

---

### Environment-Specific Values

The following legacy infrastructure values must be systematically sourced rather than hardcoded:

| Legacy Source Value / Key | Description | Classification | Target Platform Sourcing / Normalization |
| :--- | :--- | :--- | :--- |
| `HostDst` (`DWHDWH1P`) | Execution/Polling host | GLOBAL | Sourced via environment parameters: `GCP_PROJECT` / `GCP_REGION` |
| `Login` (`DW.UNIX.ISTNS`) | SFTP/SSH Connection identifier | GLOBAL | Resolved as an Airflow Connection ID: `sftp_dwh_conn` |
| `/app_dwh/sftp_users/istcomis/daten/tcom/iar/work/` | Inbound directory for SFTP | GLOBAL | Mapped to `GCS_BUCKET` pathing (or `SFTP_BASE_PATH` if SFTP host is maintained in GCP) |
| `CallOP` (`DW.CALL_STANDARD`) | Standard notification object | GLOBAL | Mapped to Airflow standard alerting configuration / custom utility hook |
| `StartJp` (`DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP`) | Target downstream pipeline | JOB-SPECIFIC | Target Dag ID: `dw_dwh_iar_bgf_gutschrift_import_jp` |

---

### Risks & Manual Actions

1. **Downstream Pipeline Migration Dependence**: The downstream workflow `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP` is not migrated under this design pass. The `TriggerDagRunOperator` must be validated against its final, target-side sanitized DAG ID once it has been migrated and deployed.
2. **Missing Include Object**: `: inc DW.HOLE_PFAD` is not part of this pass's source files. The target-side environmental mappings must manually verify that the base paths are correctly aligned with the assumptions in this include object.
3. **SFTP Connection Setup**: The connection `DW.UNIX.ISTNS` must be provisioned inside Airflow/Cloud Composer with the correct SSH/SFTP private keys to query files from host `DWHDWH1P`.

---

### Hard Rules & Output/Print Literal Rule

Pursuant to the **OUTPUT/PRINT LITERAL RULE**, all logging/print messages extracted from the legacy source must retain their exact original wording and characters. 

The following literal outputs must be emitted by the DAG's logging/print blocks verbatim:
- `: if SYS_STATE_ACTIVE(JOBP, &StartJp) = 'Y'` print block:
  ```text
  Jobplan DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP is active!
  ```
- `: else` print block (trigger initiation):
  ```text
  Starting Jobplan DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP ...
  ```
- Trigger success logging:
  ```text
  JP started at {date} ...
  ```