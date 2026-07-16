# MIGRATION_NOTES.md

## 1. Summary
The `DW.DWH_STAMM_KNZB_ABGL_JP` job plan and its associated components have been migrated from **UC4 / Automic** to **Google Cloud Platform (GCP)**. 

* **Source Platform:** UC4 / Automic (Job Plan `JOBP` and Job Scripts `JOBS`)
* **Target Platform:** Google Cloud Platform (Cloud Composer / Apache Airflow)
* **Migration Pattern:** `UC4_ONLY` (Orchestration-only migration. No data layer migration or storage conversion is directly initiated within this orchestration scope).
* **Functional Scope:** Daily reconciliation and matching of Customer Number and Base Access master data (*Kundennummer-/Basiszugangs-Stammdaten*, KNZB) between the ISTNS source system and the DWH Core Layer (*DWH-Kernschicht*).

---

## 2. Generated Artifacts
The following files have been generated to replicate the original UC4 structure while adhering to the **Folder Integrity Rule**:

| Target File Path | Role / Description |
| :--- | :--- |
| `dags/dw_dwh_stamm_knzb_abgl_jp.py` | **Master Orchestration DAG**: Defines the linear execution sequence of the reconciliation pipeline. |
| `dags/dw_dwh_stamm_knzb_abgl_start_js.py` | **Initialization DAG**: Performs pre-execution state checks, verifies concurrency locks, and sets run metadata. |
| `dags/dw_dwh_stamm_knzb_abgl_ende_js.py` | **Completion DAG**: Resets the concurrency lock state back to `"FREI"` and logs completion metrics. |
| `plugins/helpers/hole_pfad_knzb.py` | **Shared Utility Module**: Resolves environment-specific directory paths (`DWH_HOME`, `HOME`, `ISTNS_HOME`) from Airflow Variables. |
| `plugins/helpers/lese_log_knzb.py` | **Shared Audit Hook**: Implements a reusable callback to write standardized UC4-style execution logs. |

---

## 3. Key Design Decisions

### Concurrency and State Management
* **Airflow Variables as State Store:** The original UC4 workflow used database-driven `GET_VAR` and `PUT_VAR` statements against the `DW.VARIABLEN_KNZB` container. This has been mapped to native Airflow JSON Variables (`Variable.get` / `Variable.set`) under the key `dw_variablen_knzb`.
* **Serialization Safety:** To prevent race conditions and concurrent state updates, `max_active_runs=1` is strictly enforced across all three generated DAGs.
* **Locking Guard:** The initialization DAG (`dw_dwh_stamm_knzb_abgl_start_js`) acts as a protective synchronization gate. If the variable `abgleich_status` is set to `"GESPERRT"`, the DAG raises an `AirflowSkipException` to halt downstream execution safely without triggering false critical alerts.

### Modularity and Code Reuse
* **Plugin Architecture:** Rather than duplicating code or merging files across directories, the UC4 `:inc` (include) commands are cleanly refactored into Python modules (`plugins/helpers/`).
* **Audit Trail Preservation:** The original German logging formats (e.g., `"Protokolleintrag: [Task] innerhalb [DAG]"`) are preserved character-for-character using Airflow's `on_execute_callback` hook.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variables Creation
Before triggering any of the migrated DAGs, the following variables must be configured in the Cloud Composer environment (via Airflow UI **Admin -> Variables** or the `gcloud composer environments run` CLI):

#### Variable 1: `dw_variablen` (JSON)
Stores global environment paths. Map these to your target Google Cloud Storage (GCS) buckets:
```json
{
  "DWH_HOME": "gs://your-dwh-home-bucket/dwh",
  "HOME": "gs://your-home-bucket/home",
  "ISTNS_HOME": "gs://your-istns-home-bucket/istns"
}
```

#### Variable 2: `dw_variablen_knzb` (JSON)
Stores the state machine variables for the KNZB reconciliation process:
```json
{
  "abgleich_status": "FREI",
  "letzter_lauf": "20260101"
}
```

### 2. IAM & Permissions
Ensure that the Cloud Composer Environment Service Account has the following permissions:
* `roles/composer.worker` (Standard worker execution)
* Storage Object Viewer/Creator permissions on the GCS buckets mapped in `dw_variablen`.

### 3. Scheduling & Upstream Triggers
Because the original UC4 Job Plan was configured with `AllowExternal="1"` and had no automatic calendar schedules, the master DAG `dw_dwh_stamm_knzb_abgl_jp` is deployed with `schedule_interval=None`. 
* If this job needs to run daily, configure a cron schedule (e.g., `schedule="0 2 * * *"`) in `dw_dwh_stamm_knzb_abgl_jp.py`.
* Alternatively, trigger it via an upstream DAG using a `TriggerDagRunOperator`.

---

## 5. Known Gaps & Unresolved References
* **Redesign (B4) Items:** The state machine relies on Airflow Variables stored in the Airflow Metadata Database. For high-frequency or highly concurrent environments, this can cause database lock contention. 
  * *Recommendation:* If performance issues arise, migrate the state tracking from Airflow Variables to a dedicated metadata table in **BigQuery** or **Cloud SQL**.
* **Path Mapping:** The paths resolved by `hole_pfad_knzb.py` default to placeholder GCS buckets (`gs://your-*-bucket/...`). These must be updated to point to actual project buckets during deployment.

---

## 6. Validation

### How to Run the Tests
1. **Dry Run / DAG Parse Test:**
   Verify that Airflow can parse the DAGs without syntax or import errors:
   ```bash
   python3 dags/dw_dwh_stamm_knzb_abgl_jp.py
   python3 dags/dw_dwh_stamm_knzb_abgl_start_js.py
   python3 dags/dw_dwh_stamm_knzb_abgl_ende_js.py
   ```
2. **Unit Test State Transitions:**
   * Set `abgleich_status` to `"FREI"` in the Airflow UI. Trigger `dw_dwh_stamm_knzb_abgl_start_js`. Verify that the status changes to `"LAEUFT"`.
   * Set `abgleich_status` to `"GESPERRT"`. Trigger `dw_dwh_stamm_knzb_abgl_start_js`. Verify that the task `check_and_update_status` skips and does not fail.
   * Trigger `dw_dwh_stamm_knzb_abgl_ende_js`. Verify that the status resets back to `"FREI"`.

### What "Passing" Means
* The master DAG `dw_dwh_stamm_knzb_abgl_jp` executes successfully from end-to-end.
* Task logs display the exact audit entry:
  `INFO - Protokolleintrag: [task_id] innerhalb [dag_id]`
* The final state of `dw_variablen_knzb` has `"abgleich_status": "FREI"` and `"letzter_lauf"` updated to the execution date (format `YYYYMMDD`).

---

## 7. Rollback Procedure
In the event of a deployment failure or unexpected behavior:

1. **Pause the Migrated DAGs:**
   Disable the DAGs via the Airflow CLI or UI:
   ```bash
   gcloud composer environments run <env-name> \
       --location <location> \
       dags pause -- dw_dwh_stamm_knzb_abgl_jp
   ```
2. **Re-enable UC4 Execution:**
   Re-activate the original UC4 Job Plan `DW.DWH_STAMM_KNZB_ABGL_JP` in the Automic UI.
3. **Reset State Variables:**
   If the migration failed mid-run, manually reset the state variable in UC4 or Airflow to prevent locking issues on the next run.