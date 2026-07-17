# MIGRATION_NOTES.md

## 1. Summary
This document details the migration of the legacy UC4 / Automic workflow job `DW.DWH_VERTRAG_TARIF_SYNC_JP` to **Google Cloud Composer (Apache Airflow)**. 

The source workflow orchestrated state variables, paths, and metadata logging for the weekly contract and tariff synchronization process (`VERTRAG_TARIF_SYNC`). Following the **UC4_ONLY** migration pattern, the legacy XML job definitions and include scripts have been refactored into a native Python-based Airflow DAG and reusable utility modules.

*   **Source Platform:** UC4 / Automic Workflows
*   **Target Platform:** Google Cloud Composer (Apache Airflow)
*   **Migration Pattern:** UC4_ONLY (Pure orchestration, state management, and logging)

---

## 2. Generated Artifacts
The migration process generated three target files, preserving the original folder structure and modular design:

| Target File Path | Role / Description |
| :--- | :--- |
| `dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/dw_dwh_vertrag_tarif_sync_jp.py` | **Primary Airflow DAG File.** Consolidates the orchestration logic from the legacy workflow, including the start (`START_JS`) and end (`ENDE_JS`) task logic. |
| `dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/includes/dw_hole_pfad_vtrg.py` | **Path Resolution Utility.** A refactored Python module that retrieves global environment paths (`DWH_HOME`, `HOME`, `PMS_HOME`) from Airflow Variables. |
| `dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/includes/dw_lese_log_vtrg.py` | **Logging Utility.** A refactored Python module that outputs standardized execution logs to the Airflow task log. |

---

## 3. Key Design Decisions

### UC4_ONLY Migration Pattern
Because the source XMLs only managed state variables, path resolution, and execution logging (without containing direct SQL or data-loading scripts), the migration focuses purely on orchestration. No data-loader scripts were generated.

### State Management via Airflow Variables
The legacy workflow relied on a central variable container (`DW.VARIABLEN_VTRG`) to track the synchronization state and prevent concurrent runs. This has been mapped to **Airflow Variables**:
*   `DW_VARIABLEN_VTRG__SYNC_STATUS`: Acts as a semaphore gate (`FREI`, `LAEUFT`, `GESPERRT`).
*   `DW_VARIABLEN_VTRG__LETZTER_LAUF`: Tracks the execution date (`YYYYMMDD`) of the last successful run.

### Folder Integrity & Modularization
To maintain clean code and reusability, the legacy include scripts (`DW.HOLE_PFAD_VTRG` and `DW.LESE_LOG_VTRG`) were not hardcoded into the DAG. Instead, they were refactored into a mirrored `includes` subdirectory and imported as standard Python modules.

### Strict Output Preservation
All log messages and console outputs have been preserved character-for-character in their original German language to ensure compatibility with downstream log parsers or operational monitoring tools.

---

## 4. Manual Steps Before Go-Live

### A. Airflow Variable Registration
Before executing the DAG, you must register the following variables in the Airflow Environment (**Admin -> Variables**):

| Variable Name | Expected Value / Format | Description |
| :--- | :--- | :--- |
| `DWH_HOME` | `/home/airflow/gcs/dags` (or environment equivalent) | Root directory path for DWH assets. |
| `HOME` | `/home/airflow` | User home directory path. |
| `PMS_HOME` | `/home/airflow/pms` | PMS home directory path. |
| `DW_VARIABLEN_VTRG__SYNC_STATUS` | `FREI` | Initial semaphore state. |
| `DW_VARIABLEN_VTRG__LETZTER_LAUF` | `19700101` (or last successful legacy run date) | Seed value for tracking the last run. |

### B. IAM & Permissions
Ensure that the Cloud Composer Service Account has the **Composer Worker** role and sufficient permissions to read and write Airflow Variables dynamically during task execution.

### C. Scheduling Verification
The DAG is configured to run weekly on **Sundays at 06:00 UTC** (`0 6 * * 0`). Verify that this schedule does not conflict with upstream data preparation tasks in the target environment.

---

## 5. Known Gaps & Unresolved References
*   **Downstream Synchronization Actions:** This DAG only manages the *orchestration state* (setting status to `LAEUFT` and resetting to `FREI`). The actual data synchronization processes (which occurred externally or via DB links in the legacy environment) must be scheduled downstream of `start_task` or triggered via Airflow Dataset/DAG dependencies.
*   **Hardcoded Start Date:** The DAG's `start_date` is set to `2024-12-01`. This should be adjusted to the actual migration go-live date.

---

## 6. Validation

### A. How to Run the Tests
1. **DAG Parsing Test:** Verify that the Airflow environment can parse the DAG without syntax or import errors:
   ```bash
   python3 dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/dw_dwh_vertrag_tarif_sync_jp.py
   ```
2. **Task-Level Test:** Test individual tasks using the Airflow CLI:
   ```bash
   airflow tasks test DW_DWH_VERTRAG_TARIF_SYNC_JP start_task 2024-12-08
   airflow tasks test DW_DWH_VERTRAG_TARIF_SYNC_JP ende_task 2024-12-08
   ```

### B. What "Passing" Means
*   **`start_task` Success:**
    *   If `DW_VARIABLEN_VTRG__SYNC_STATUS` is `FREI`, the task sets the variable to `LAEUFT`, updates `DW_VARIABLEN_VTRG__LETZTER_LAUF` to the execution date, and exits successfully.
    *   If `DW_VARIABLEN_VTRG__SYNC_STATUS` is `GESPERRT`, the task prints `"Vertrags-/Tarifabgleich fuer <Datum> ist gesperrt - Abbruch"` and raises an `AirflowFailException`.
*   **`ende_task` Success:**
    *   Resets `DW_VARIABLEN_VTRG__SYNC_STATUS` back to `FREI`.
    *   Prints `"Vertrags-/Tarifabgleich fuer Lauf <Datum> erfolgreich beendet"`.

---

## 7. Rollback Procedure
In the event of a deployment failure or critical runtime issue:

1. **Pause the DAG:** Immediately pause the DAG in the Airflow UI or via the CLI:
   ```bash
   airflow dags pause DW_DWH_VERTRAG_TARIF_SYNC_JP
   ```
2. **Reset State Variables:** Manually reset the Airflow Variables to a safe state via the Airflow Admin UI:
   *   Set `DW_VARIABLEN_VTRG__SYNC_STATUS` to `FREI`.
3. **Remove Artifacts:** Delete the generated files from the Cloud Composer GCS bucket:
   ```bash
   gsutil rm gs://<composer-bucket>/dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/dw_dwh_vertrag_tarif_sync_jp.py
   gsutil rm -r gs://<composer-bucket>/dags/DWH_KERN/PRODUKTION/DW_DWH_VERTRAG/includes/
   ```