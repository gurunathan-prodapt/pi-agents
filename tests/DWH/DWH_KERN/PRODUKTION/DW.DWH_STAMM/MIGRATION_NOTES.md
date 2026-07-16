# MIGRATION_NOTES.md

**Migration Target:** Google Cloud Platform (GCP) — Cloud Composer (Apache Airflow) & BigQuery  
**Source Job:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_JP.xml`  
**Status:** Migration Completed & Re-Engineered

---

## 1. Summary

The legacy UC4 Job Plan `DW.DWH_STAMM_KNZB_ABGL_JP` has been migrated to an Apache Airflow DAG running on Cloud Composer. 

This workflow orchestrates the daily reconciliation and alignment of customer numbers and basic access master data (Kundennummer-/Basiszugangs-Stammdaten, or **KNZB**) between the legacy `ISTNS` source system and the Core Data Warehouse layer (`DWH-Kernschicht`). 

The migration translates the sequential execution of the start and end wrapper scripts, preserves the state-locking mechanism using Airflow Variables, and modularizes shared logic into reusable Python helpers.

---

## 2. Generated Artifacts

The migration process generated the following files, structured for deployment within a Cloud Composer environment:

| Artifact Path | Role | Description |
| :--- | :--- | :--- |
| **`dags/dw_dwh_stamm_knzb_abgl_jp.py`** | Main DAG | Orchestrates the workflow sequence (`start` $\rightarrow$ `start_js` $\rightarrow$ `ende_js` $\rightarrow$ `end`). |
| **`tasks/dw_dwh_stamm_knzb_abgl_start_js.py`** | Task Script | Executes the start-alignment logic, checks for execution locks, and sets state flags. |
| **`tasks/dw_dwh_stamm_knzb_abgl_ende_js.py`** | Task Script | Executes the end-alignment logic, releases execution locks, and logs completion. |
| **`utils/knzb_helpers.py`** | Shared Utility | Consolidates the migrated UC4 includes `DW.HOLE_PFAD_KNZB` and `DW.LESE_LOG_KNZB` into reusable Python functions. |

---

## 3. Key Design Decisions

* **PythonOperator vs. DataprocSubmitJobOperator:** While the initial automated MCP output assumed a Dataproc/PySpark execution model, the actual script logic extracted from the XML files revealed lightweight state-checking, variable manipulation, and logging. Consequently, these tasks were re-engineered as native Python functions executed via the `PythonOperator` directly within Cloud Composer. This avoids the overhead of spinning up or submitting jobs to a Dataproc cluster.
* **State Management via Airflow Variables:** The legacy UC4 process relied on global variable containers (`DW.VARIABLEN_KNZB`) to manage execution locks (`GESPERRT`, `LAEUFT`, `FREI`). This state-locking mechanism is preserved natively using Airflow Variables (`dw_variablen_knzb_abgleich_status` and `dw_variablen_knzb_letzter_lauf`).
* **Consolidation of Includes:** The legacy includes `DW.HOLE_PFAD_KNZB` (path resolution) and `DW.LESE_LOG_KNZB` (standardized logging) were refactored into a shared utility module (`utils/knzb_helpers.py`) to prevent code duplication across the start and end tasks.
* **Preservation of German Log Literals:** To maintain compatibility with legacy log parsers and operational runbooks, all German log messages and output formats have been preserved verbatim.

---

## 4. Manual Steps Before Go-Live

The following configuration steps must be performed in the target Cloud Composer environment prior to triggering the DAG:

### 1. Airflow Variables Seeding
Create the following Airflow Variables via the Airflow UI (**Admin -> Variables**) or the `gcloud composer environments run` CLI:

| Variable Key | Expected Initial Value | Description |
| :--- | :--- | :--- |
| `dw_variablen_knzb_abgleich_status` | `FREI` | Controls execution lock. Allowed values: `FREI`, `LAEUFT`, `GESPERRT`. |
| `dw_variablen_knzb_letzter_lauf` | `YYYYMMDD` (e.g., `20260715`) | Date of the last successful run. |
| `DWH_HOME` | `/home/gurunathan_t/clean_migration_dataset` | Path referencing core environment resources. |
| `HOME` | `/home/gurunathan_t` | Home directory path for user executions. |
| `ISTNS_HOME` | `/home/gurunathan_t/istns` | Connection/interface path for the ISTNS source system. |

### 2. Environment Variables
Ensure the following environment variables are set in your Cloud Composer environment:
* `GCP_PROJECT`: Your target Google Cloud Project ID.
* `BQ_DATASET`: Target BigQuery Core Schema/Dataset (defaults to `DW_DWH_STAMM`).

### 3. IAM Permissions
The Cloud Composer Service Account must have the following permissions:
* **Storage Object Viewer** (`roles/storage.objectViewer`) on the environment's GCS bucket.
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) and **BigQuery Job User** (`roles/bigquery.jobUser`) if downstream tasks interact with BigQuery.

---

## 5. Known Gaps & Unresolved References

* **External Scheduling:** No scheduling context (such as a UC4 `JSCH` or `EVNT_TIME`) was provided in the source XML. The DAG is currently configured with `schedule_interval=None`. If this workflow must run on a daily cadence, update the `schedule_interval` in `dags/dw_dwh_stamm_knzb_abgl_jp.py` to the desired cron expression (e.g., `0 2 * * *` for daily at 2:00 AM).
* **Hardcoded Paths:** The default paths in `utils/knzb_helpers.py` point to `/home/gurunathan_t/...`. While these are safely overridden by seeding the `DWH_HOME`, `HOME`, and `ISTNS_HOME` Airflow Variables, it is recommended to update these fallback defaults to match your production directory structure.

---

## 6. Validation

### 1. Unit Testing Task Logic
You can test the individual task execution blocks locally or within a Composer worker terminal:

```bash
# Test the Start Task (Should set status to LAEUFT and set last run date)
airflow tasks test dw_dwh_stamm_knzb_abgl_jp dw_dwh_stamm_knzb_abgl_start_js 2026-07-16

# Test the End Task (Should reset status to FREI)
airflow tasks test dw_dwh_stamm_knzb_abgl_jp dw_dwh_stamm_knzb_abgl_ende_js 2026-07-16
```

### 2. Lock Verification Test
1. Set the Airflow Variable `dw_variablen_knzb_abgleich_status` to `GESPERRT`.
2. Trigger the DAG.
3. **Expected Result:** The task `dw_dwh_stamm_knzb_abgl_start_js` must fail immediately with an `AirflowFailException`, logging:  
   `"KNZB-Abgleich fuer <DATUM> ist gesperrt - Abbruch der Verarbeitung"`.

### 3. Happy Path Verification Test
1. Set the Airflow Variable `dw_variablen_knzb_abgleich_status` to `FREI`.
2. Trigger the DAG.
3. **Expected Result:** 
   * `dw_dwh_stamm_knzb_abgl_start_js` succeeds, setting the variable to `LAEUFT`.
   * `dw_dwh_stamm_knzb_abgl_ende_js` succeeds, resetting the variable back to `FREI`.
   * The DAG run completes with status `SUCCESS`.

---

## 7. Rollback Procedure

In the event of an operational failure or the need to revert to the legacy UC4 orchestration:

1. **Pause the Airflow DAG:**
   ```bash
   gcloud composer environments run <ENVIRONMENT_NAME> \
       --location <LOCATION> \
       dags pause -- dw_dwh_stamm_knzb_abgl_jp
   ```
2. **Reset the Lock Variable:** Ensure the state variable is set back to `FREI` to prevent locking out legacy runs:
   * Go to Airflow UI -> Admin -> Variables.
   * Edit `dw_variablen_knzb_abgleich_status` and set its value to `FREI`.
3. **Re-enable UC4 Job Plan:** Reactivate the `DW.DWH_STAMM_KNZB_ABGL_JP` workflow in the UC4/Automic UI.