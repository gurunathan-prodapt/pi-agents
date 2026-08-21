=== OBJECT: DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT (EVNT_FILE) ===
active=1
title=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Migration Design Document: UC4 to Apache Airflow

## 1. Overview
The provided extraction bundle consists of a single UC4 Event object of type `EVNT_FILE` named `DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT`. In UC4, an `EVNT_FILE` monitors a file system or external storage for the arrival of a specific file and subsequently triggers a target workflow or processing job when the file is detected. Because no accompanying workflow (`JOBP`) or execution job (`JOBS`) was supplied in this extraction, the downstream logic, file paths, and target systems cannot be fully determined. This DAG will act as an event-sensing stub that needs to be configured with the actual target storage/path and downstream trigger.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT` | EVNT_FILE | Active (1) | None |

---

## 3. Scheduling
- **Schedule:** `None`
- **Trigger Source:** This workflow represents an event-driven file trigger (`EVNT_FILE`). It is externally triggered by the arrival of a physical file, rather than running on a traditional calendar-based cron schedule.
- **Airflow Mapping:** The DAG is configured with `schedule=None`. In a production environment, this should either be triggered via Airflow Datasets (upon file delivery) or run continuously as a sensor-based DAG.

---

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_run_iar_bgf_gutschrift_import_jp_evt` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `detect_file_event` | `DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT` | `EmptyOperator` | N/A | N/A | 1 | 5 minutes | N/A | N/A | N/A | N/A | **# REVIEW-STRUCT:** This task represents a UC4 file event. The exact file path, directory, and protocol (e.g., GCS, SFTP, local) are not supplied. Replace this `EmptyOperator` with an appropriate sensor (e.g., `GCSObjectExistenceSensor` or `SFTPSensor`) once file details are confirmed. |

---

## 6. Task Dependency Map
```python
detect_file_event
```
*(No downstream dependencies or target workflows are defined within this extraction bundle).*

---

## 7. Sync / Concurrency Analysis
No sync rows, locks, or cross-DAG exclusions were supplied for this object. The DAG-level concurrency limit of `max_active_runs=1` is implemented by default to prevent concurrent runs from overlapping while polling for the file.

---

## 8. Error Handling and Retry Strategy
- **Retries:** Standard retries are set to `1` with a `5-minute` delay.
- **Postconditions:** No explicit UC4 postconditions, alerts, or script callbacks were found. Standard Airflow failure handling will apply.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT` | Object Name | `dw_dwh_run_iar_bgf_gutschrift_import_jp_evt` |

---

## 10. Developer Notes
* **UNCERTAIN:** This extraction contains only a single file event receiver. The actual target process or workflow that UC4 triggers upon successful file detection is missing from this extraction.
* **# REVIEW-STRUCT:** The file path, pattern, host, and storage mechanism (e.g., Cloud Storage, local file system, SFTP server) are missing. The developer must replace the `EmptyOperator` (`detect_file_event`) with a specialized sensor such as `GCSObjectExistenceSensor`, `SFTPSensor`, or `FileSensor`, and supply the real path connection parameters.
* **# REVIEW-STRUCT:** A downstream task, such as a `TriggerDagRunOperator`, must be appended to this DAG to invoke the target processing pipeline once the sensor successfully detects the file.

---

# Pseudocode Outline

```python
# ==============================================================================
# 1. Imports
# ==============================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# # REVIEW-STRUCT: Import specific sensor depending on actual target environment:
# from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
# from airflow.providers.sftp.sensors.sftp import SFTPSensor

# ==============================================================================
# 2. GCP / Environment Configuration (Placeholders)
# ==============================================================================
# # REVIEW-STRUCT: Define file path and bucket details once target design is finalized.
# GCS_BUCKET = "your-gcs-bucket-placeholder"
# FILE_PATH = "path/to/iar_bgf_gutschrift_import_file.csv"

# ==============================================================================
# 3. Default Args
# ==============================================================================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# 4. DAG Definition
# ==============================================================================
with DAG(
    dag_id="dw_dwh_run_iar_bgf_gutschrift_import_jp_evt",
    default_args=default_args,
    description="File event sensor DAG for IAR BGF Gutschrift Import",
    schedule_interval=None,  # Event-driven execution
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["uc4_migration", "event_file"],
) as dag:

    # ==========================================================================
    # 5. Task Definitions
    # ==========================================================================
    
    # # REVIEW-STRUCT: Implement actual sensor operator here instead of EmptyOperator.
    # Example for GCS:
    # detect_file_event = GCSObjectExistenceSensor(
    #     task_id="detect_file_event",
    #     bucket=GCS_BUCKET,
    #     object=FILE_PATH,
    #     poke_interval=300,
    #     timeout=3600,
    # )
    
    detect_file_event = EmptyOperator(
        task_id="detect_file_event",
    )

    # ==========================================================================
    # 6. Dependencies
    # ==========================================================================
    # No downstream dependencies provided in extraction bundle.
    detect_file_event
```

# Migration Design Document

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/EINMALIG/DWH_2020/DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP/DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT.xml` | `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/EINMALIG/DWH_2020/DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP/DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT.py` | Converts the UC4 file monitoring event and activation script into an event-driven Airflow DAG. |

---

## 1. Job Dependencies
- **Downstream Consumer:** `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP` (defined by variable `&StartJp` in the UC4 script).
  - **Airflow Wiring:** Once the file event sensor detects the file, it must trigger the downstream workflow via a `TriggerDagRunOperator` targeted at `dw_dwh_iar_bgf_gutschrift_import_jp`.
  - **Migration Status:** The downstream workflow is not yet migrated in this scope (see *Risks & Manual Actions*).

---

## 2. Scheduling
- **Trigger Event:** File arrival.
- **Airflow Mapping:** The DAG is configured with `schedule=None` (event-driven). The physical monitoring of the file is done either by a continuous Airflow sensor (using reschedule mode to save resources) or triggered externally by a Cloud Function/Eventarc event reacting to file uploads.

---

## 3. Schedule & Variables
- **Trigger Source:** UC4 File Event `DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT`
- **Equivalent Trigger:** Sensed via `SFTPSensor` or `GCSObjectExistenceSensor` targeting `/app_dwh/sftp_users/istcomis/daten/tcom/iar/work/DWHK_DWHM_IAR_GUTSCHR_*.chk`.
- **Variables Retained:**
  - `Executor = 'DWHDWH1P'`: Sourced as a connection ID or variable in Airflow.
  - `StartJp = 'DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP'`: Mapped to target DAG ID `dw_dwh_iar_bgf_gutschrift_import_jp`.
  - `date = "SYS_DATE('JJJJMMTT')"`: Formatted using Jinja in Airflow: `{{ ds_nodash }}`.
  - `AKTOBJ = 'ACTIVATE_UC_OBJECT(JOBP,&StartJp)'`: Mapped to an Airflow `TriggerDagRunOperator` execution.
  - `CallOP = 'ACTIVATE_UC_OBJECT(CALL,DW.CALL_STANDARD)'`: Sourced via the Airflow on-failure alert mechanism (e.g. SMTP or Slack hook).

---

## 4. Lineage
- **Upstream Producer:** `OTHER:/APP_DWH/SFTP_USERS/ISTCOMIS/DATEN/TCOM/IAR/WORK/DWHK_DWHM_IAR_GUTSCHR_*.CHK` on host `EXT:dwhdwh1p` (external SFTP source).
- **Downstream Consumer:** `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP` (job: `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP`), representing the processing pipeline that consumes the file.

---

## 5. Cross-File Dependencies
- **Include Script:** `: inc DW.HOLE_PFAD` is referenced in the UC4 script block to load paths/environment definitions.

---

## 6. Target File Plan
- **Target File Path:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/EINMALIG/DWH_2020/DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP/DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT.py`
  - **Language:** Python (Apache Airflow DAG)
  - **Source File:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/EINMALIG/DWH_2020/DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP/DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT.xml`

---

## 7. Environment-Specific Values

### GLOBAL
- **`DWHDWH1P` (Host):** Identifies the target infrastructure server. Sourced at runtime via Airflow Connection parameters (e.g., `SFTP_CONN_ID`).
- **`DW.UNIX.ISTNS` (Login):** Legacy credentials container. Sourced at runtime via Composer/Airflow connection secrets.
- **`DW.CALL_STANDARD` (Alert):** Global operator call. Maps to the Composer-wide notification callback configuration.

### JOB-SPECIFIC
- **`Path` (`/app_dwh/sftp_users/istcomis/daten/tcom/iar/work/DWHK_DWHM_IAR_GUTSCHR_*.chk`):** The search pattern for incoming check files. Kept as a specific task parameter inside the sensor definition.
- **`StartJp` (`DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP`):** The downstream DAG to invoke. Stored as a target `trigger_dag_id` in the `TriggerDagRunOperator`.

---

## 8. Risks & Manual Actions
- SOURCE: NOT FOUND — `DW.HOLE_PFAD` — no candidate
- The downstream consumer `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP` is not yet migrated in this scope. The wiring via `TriggerDagRunOperator` cannot be finalized or validated until this target DAG is deployed in the Airflow environment.
- File system protocol: The UC4 Event monitors files on a UNIX filesystem (`DWHDWH1P`). The developer must clarify whether the files will land in GCS or an SFTP server in the target cloud model, and configure either a `GCSObjectExistenceSensor` or `SFTPSensor` accordingly.