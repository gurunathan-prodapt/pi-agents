# Migration Notes: `DW.DWH_VERTRAG_TARIF_SYNC_JP.xml`

This document outlines the migration details, design decisions, manual setup steps, and validation procedures for migrating the weekly contract/tariff synchronization job plan from UC4 to Google Cloud Composer (Apache Airflow).

---

## 1. Summary

* **Source Workflow**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_JP.xml` (and associated job scripts/includes).
* **Target Platform**: Google Cloud Composer (Apache Airflow).
* **Migration Pattern**: `UC4_ONLY` orchestration migration.
* **Functional Scope**: This workflow orchestrates a weekly synchronization and alignment of contract and tariff allocations (`Vertrags-/Tarifzuordnung`) between the source system (`STAMMDATEN`) and the Core Data Warehouse layer (`DWH_KERN`). It manages execution locks and state variables to prevent concurrent runs and track execution history.

---

## 2. Generated Artifacts

The following files have been generated to replace the legacy UC4 XML components:

| Target File Path | Role | Description |
| :--- | :--- | :--- |
| `dags/dw_dwh_vertrag_tarif_sync_jp.py` | **DAG Orchestrator** | Defines the Airflow DAG structure, scheduling (`@weekly`), and linear task dependencies. |
| `dags/tasks/dw_dwh_vertrag_tarif_sync_start.py` | **Task Script (Start)** | Implements the start-control block logic, checking the lock status and setting state variables. |
| `dags/tasks/dw_dwh_vertrag_tarif_sync_ende.py` | **Task Script (End)** | Implements the end-control block logic, resetting the lock status and logging completion. |
| `dags/tasks/utils.py` | **Shared Utilities** | Houses merged include logic for path resolution (`DW.HOLE_PFAD_VTRG`) and standard logging (`DW.LESE_LOG_VTRG`). |

---

## 3. Key Design Decisions

### A. Variable Control Mechanism (UC4 `GET_VAR` / `PUT_VAR` Replacement)
* **Decision**: Map UC4 global variables to **Airflow Variables** stored in the Airflow metadata database.
* **Reasoning**: Airflow Variables provide a native, persistent, and thread-safe mechanism to manage state across tasks and DAG runs without requiring external database tables.
* **Mapping**:
  * `DW.VARIABLEN_VTRG` / `SYNC_STATUS` $\rightarrow$ `vtrg_sync_status` (Values: `FREI`, `LAEUFT`, `GESPERRT`).
  * `DW.VARIABLEN_VTRG` / `LETZTER_LAUF` $\rightarrow$ `vtrg_letzter_lauf` (Value: `YYYYMMDD`).

### B. Shared Include Consolidation
* **Decision**: Consolidate `DW.HOLE_PFAD_VTRG` (path resolution) and `DW.LESE_LOG_VTRG` (standard logging) into a shared Python module (`tasks/utils.py`).
* **Reasoning**: This avoids code duplication across task files while strictly preserving the **OUTPUT/PRINT LITERAL RULE** required for downstream log parsing and auditing.

### C. Concurrency and Execution Constraints
* **Decision**: Set `max_active_runs=1` and `catchup=False` in the DAG definition.
* **Reasoning**: This replicates the serial execution constraints of the original UC4 environment, preventing race conditions on the `vtrg_sync_status` semaphore variable.

---

## 4. Manual Steps Before Go-Live

The following configuration steps must be completed in the target environment before triggering the DAG:

### A. Airflow Variables Setup
Create the following variables in the Airflow UI (**Admin -> Variables**) or via the Airflow CLI:

| Variable Key | Initial/Default Value | Description |
| :--- | :--- | :--- |
| `DWH_HOME` | `/opt/dwh` | Global system path for DWH home. |
| `HOME` | `/home/airflow` | Global system path for Airflow home. |
| `PMS_HOME` | `/opt/pms` | Global system path for PMS home. |
| `vtrg_sync_status` | `FREI` | Semaphore lock. Must be initialized to `FREI` to allow the first run. |
| `vtrg_letzter_lauf` | `19700101` | Placeholder date for the last successful run. |

### B. IAM & Permissions
* Ensure the Cloud Composer Service Account has read/write permissions to the Airflow Metadata Database (granted by default within Composer).
* If paths resolved by `resolve_environment_paths()` point to Cloud Storage buckets or external mount points, verify that the Composer worker service account has appropriate IAM permissions (e.g., `Storage Object Viewer/Creator`).

### C. Scheduling
* The DAG is configured to run weekly (`0 0 * * 0` - Sunday at midnight). Verify that this does not conflict with source system maintenance windows.

---

## 5. Known Gaps & Unresolved References

* **External Dependencies**: The workflow assumes that the source data in `STAMMDATEN` and `DWH_KERN` is updated and ready for synchronization by Sunday at midnight. There are no explicit upstream Airflow sensors configured in this DAG. If upstream data readiness must be guaranteed, an `ExternalTaskSensor` or a GCS Sensor should be added to the `start` task.
* **Hardcoded Paths**: The fallback paths in `tasks/utils.py` (`/opt/dwh`, `/home/airflow`, `/opt/pms`) are local directory structures. If running in a fully cloud-native Cloud Composer environment, these should be updated to point to GCS bucket paths (e.g., `gs://<bucket-name>/dwh`) via the Airflow Variables UI.

---

## 6. Validation

### A. How to Run the Tests
1. **Dry Run**:
   Verify the DAG parses correctly without syntax errors:
   ```bash
   python3 dags/dw_dwh_vertrag_tarif_sync_jp.py
   ```
2. **Task-Level Testing**:
   Test individual task execution using the Airflow CLI:
   ```bash
   airflow tasks test dw_dwh_vertrag_tarif_sync_jp dw_dwh_vertrag_tarif_sync_start_js 2024-12-01
   airflow tasks test dw_dwh_vertrag_tarif_sync_jp dw_dwh_vertrag_tarif_sync_ende_js 2024-12-01
   ```

### B. What "Passing" Means
* **Successful Run**:
  * `dw_dwh_vertrag_tarif_sync_start_js` executes, reads `vtrg_sync_status` as `FREI`, updates it to `LAEUFT`, and sets `vtrg_letzter_lauf` to the current date.
  * `dw_dwh_vertrag_tarif_sync_ende_js` executes, resets `vtrg_sync_status` back to `FREI`, and logs:
    `Vertrags-/Tarifabgleich fuer Lauf <YYYYMMDD> erfolgreich beendet`.
* **Lock Prevention (Failure Path)**:
  * Manually set `vtrg_sync_status` to `GESPERRT` in the Airflow UI.
  * Trigger the DAG.
  * The task `dw_dwh_vertrag_tarif_sync_start_js` must fail immediately with an `AirflowFailException` and log:
    `Vertrags-/Tarifabgleich fuer <YYYYMMDD> ist gesperrt - Abbruch`.

---

## 7. Rollback Procedure

In the event of a deployment failure or critical runtime issue:

1. **Pause the DAG**: Immediately pause the DAG in the Airflow UI to prevent subsequent weekly runs.
2. **Reset State Variables**:
   Manually reset the Airflow Variable `vtrg_sync_status` to `FREI` to ensure the system is not left in a permanently locked state (`LAEUFT` or `GESPERRT`).
3. **Revert Code**:
   Roll back the deployed files in the Composer DAG bucket to the previous stable Git commit:
   ```bash
   gsutil cp gs://<composer-bucket>/dags/backup/dw_dwh_vertrag_tarif_sync_jp.py gs://<composer-bucket>/dags/dw_dwh_vertrag_tarif_sync_jp.py
   ```