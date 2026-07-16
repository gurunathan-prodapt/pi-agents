# Migration Notes: `DW.DWH_STAMM_KNZB_ABGL_JP`

This document provides comprehensive migration notes for transitioning the daily customer relationship master data reconciliation process (`DW.DWH_STAMM_KNZB_ABGL_JP`) from UC4 to Google Cloud Composer (Airflow).

---

## 1. Summary

The daily customer relationship master data reconciliation process (`DW.DWH_STAMM_KNZB_ABGL_JP`) has been migrated from a legacy UC4 Job Plan to a native **Google Cloud Composer (Airflow)** DAG. 

* **Source Platform:** Automic UC4 (Job Plan & Job Scripts)
* **Target Platform:** Google Cloud Composer (Airflow 2.x) / Google Cloud Platform (GCP)
* **Migration Scope:** 
  * The primary Job Plan (`DW.DWH_STAMM_KNZB_ABGL_JP`) and its associated start/end scripts (`DW.DWH_STAMM_KNZB_ABGL_START_JS`, `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS`) have been consolidated into a single orchestrating Airflow DAG.
  * Reusable UC4 Include components (`DW.HOLE_PFAD_KNZB`, `DW.LESE_LOG_KNZB`) have been extracted into separate Python helper modules to preserve modularity and folder integrity.

---

## 2. Generated Artifacts

The migration process generated three distinct Python files, preserving the original folder structure and logical boundaries:

| Target File Path | Role | Source Reference |
| :--- | :--- | :--- |
| `dags/dw_dwh_stamm_knzb_abgl_jp.py` | **Primary DAG Orchestrator:** Defines the Airflow DAG, task dependencies, and executes the core start/end logic using PythonOperators. | `DW.DWH_STAMM_KNZB_ABGL_JP.xml`<br>`DW.DWH_STAMM_KNZB_ABGL_START_JS.xml`<br>`DW.DWH_STAMM_KNZB_ABGL_ENDE_JS.xml` |
| `dags/includes/dw_hole_pfad_knzb.py` | **Helper Module:** Resolves legacy environment path variables (`DWH_HOME`, `HOME`, `ISTNS_HOME`) using Airflow Variables. | `includes/DW.HOLE_PFAD_KNZB.xml` |
| `dags/includes/dw_lese_log_knzb.py` | **Helper Module:** Implements standardized logging outputs to capture task execution context. | `includes/DW.LESE_LOG_KNZB.xml` |

---

## 3. Key Design Decisions

### Folder Integrity Rule
To prevent folder-integrity violations and maintain clean repository boundaries, files residing in the `includes/` sub-folder in UC4 were mapped to a corresponding `dags/includes/` directory. They are imported dynamically by the main DAG file.

### Pure-Orchestration Consolidation (`UC4_ONLY`)
Because this workflow contains no external OS-level shell scripts or direct database execution boundaries, it was classified as a pure-orchestration process. 
* **Decision:** Implement the logic entirely within native Airflow **PythonOperators** rather than spinning up external BashOperators or GKEPodOperators.
* **Trade-off:** This keeps the DAG lightweight and fast, avoiding container startup overhead, but executes the logic within the Airflow worker's execution context.

### State Tracking & Locking via Airflow Variables
The legacy UC4 process used global variables (`DW.VARIABLEN_KNZB`) to prevent concurrent executions and track the last run date.
* **Decision:** Map these variables directly to Airflow Variables (`dw_variablen_knzb_abgleich_status` and `dw_variablen_knzb_letzter_lauf`).
* **Trade-off:** Airflow Variables incur a small database read/write overhead. Since this DAG runs only once daily, this overhead is negligible and guarantees state persistence across DAG runs.

### Verbatim Preservation of German Log Patterns
To ensure compatibility with legacy log parsers and maintain operational familiarity for support teams, all console outputs and error messages (e.g., `"KNZB-Abgleich fuer ... ist gesperrt"`) have been preserved verbatim in German.

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG in production, the following setup steps must be completed in the target Cloud Composer environment:

### A. Airflow Variables Configuration
Import or manually create the following Airflow Variables in the Airflow Web UI (**Admin -> Variables**):

| Variable Key | Expected Value / Default | Purpose |
| :--- | :--- | :--- |
| `GCS_BUCKET` | `your-dwh-gcs-bucket-name` | Target Cloud Storage bucket for DWH operations. |
| `DWH_HOME` | `/opt/dwh` | Legacy path reference for DWH installation. |
| `HOME` | `/home/dwh_user` | Legacy path reference for the operational user. |
| `ISTNS_HOME` | `/opt/istns` | Legacy path reference for the source system. |
| `dw_variablen_knzb_abgleich_status` | `FREI` | Orchestration state lock. Must be set to `FREI` initially. |
| `dw_variablen_knzb_letzter_lauf` | `YYYYMMDD` (e.g., `20241104`) | Tracks the execution date of the last successful run. |

### B. IAM & Permissions
Ensure that the Cloud Composer Environment Service Account has the following permissions:
* **Storage Object Viewer / Creator** on the bucket defined in `GCS_BUCKET` (if files are read/written in downstream processes).
* **Secret Manager Secret Accessor** (if any environment variables are migrated to Secret Manager in the future).

### C. Scheduling & Paused State
* The DAG is configured with `is_paused_upon_creation=False` but should be verified in the Airflow UI.
* Confirm that the daily schedule (`0 6 * * *` / 06:00 UTC) does not conflict with upstream extraction pipelines from the `ISTNS` source system.

---

## 5. Known Gaps & Unresolved References

* **Concurrency Lock Scale:** The locking mechanism relies on Airflow Variables (`dw_variablen_knzb_abgleich_status`). If multiple instances of this DAG are triggered manually in rapid succession, a race condition could theoretically occur during the read-and-write phase. However, because `max_active_runs` is strictly set to `1`, this risk is mitigated.
* **Hardcoded Default Paths:** The helper module `dw_hole_pfad_knzb.py` contains fallback default paths (e.g., `/opt/dwh`). If these directories do not exist on the Composer workers and are referenced by downstream tasks, those tasks will fail. Ensure these variables are correctly overridden in the Airflow UI.

---

## 6. Validation

To validate the migration, execute the following tests in a non-production Composer environment:

### A. Static Code Analysis & DAG Import Test
Run a local syntax and import check to ensure Airflow can parse the DAG without errors:
```bash
python3 dags/dw_dwh_stamm_knzb_abgl_jp.py
```
* **Passing Criteria:** The command exits with code `0` and outputs no syntax or import errors.

### B. Task-Level Unit Testing
Test individual tasks using the Airflow CLI:
```bash
# Test the start task (simulating a clean run where status is FREI)
airflow tasks test dw_dwh_stamm_knzb_abgl_jp dw_dwh_stamm_knzb_abgl_start_js 2024-11-04

# Test the end task
airflow tasks test dw_dwh_stamm_knzb_abgl_jp dw_dwh_stamm_knzb_abgl_ende_js 2024-11-04
```
* **Passing Criteria:** 
  * `dw_dwh_stamm_knzb_abgl_start_js` sets `dw_variablen_knzb_abgleich_status` to `LAEUFT` and exits successfully.
  * `dw_dwh_stamm_knzb_abgl_ende_js` sets `dw_variablen_knzb_abgleich_status` back to `FREI` and prints:
    `"KNZB-Stammdatenabgleich fuer Lauf <date> erfolgreich beendet"`.

### C. Lock Verification Test
1. Manually set the Airflow Variable `dw_variablen_knzb_abgleich_status` to `GESPERRT`.
2. Trigger the DAG.
* **Passing Criteria:** The task `dw_dwh_stamm_knzb_abgl_start_js` must fail immediately, raising an `AirflowFailException` with the log message:
  `"KNZB-Abgleich fuer <date> ist gesperrt - Abbruch der Verarbeitung"`.

---

## 7. Rollback Procedure

If issues are encountered in production after go-live, perform the following steps to roll back:

1. **Pause the Airflow DAG:**
   Go to the Airflow Web UI and toggle the switch next to `dw_dwh_stamm_knzb_abgl_jp` to **Off** (Paused).
2. **Re-enable the UC4 Job Plan:**
   In the Automic UC4 interface, locate the Job Plan `DW.DWH_STAMM_KNZB_ABGL_JP` and set its status to **Active** / **Scheduled**.
3. **Reset State Variables:**
   Ensure the legacy UC4 variable object `DW.VARIABLEN_KNZB` is synchronized with the last successful run state to prevent duplicate processing or gaps.