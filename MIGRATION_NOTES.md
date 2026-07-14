# Migration Notes

**Job:** Shared Files — `isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/ALLGEMEIN/INCLUDES`  
**Target Platform:** Cloud Composer (Google Cloud Managed Airflow) & Google Cloud Storage  
**Migration Pattern:** `UC4_ONLY` (Environment Setup, Date Calculus, and Operational Monitoring Includes)

---

## 1. Summary

This migration covers the transition of two critical, reusable legacy UC4 Include Scripts (`JOBI`) into a modern, cloud-native orchestration layer on **Cloud Composer (Airflow)**:
*   **`DW.HOLE_PFAD`**: Responsible for setting up directory paths, checking system activation flags (`AKTIV_*`), and calculating dynamic date windows (current, previous, and next-month boundaries).
*   **`DW.LESE_LOG`**: Responsible for intercepting job failures, extracting execution logs via legacy utilities (`showlog`), and updating operational monitoring tables.

These includes have been refactored from legacy UNIX shell/UC4 macro syntax into modular, reusable Python modules integrated directly into an Airflow DAG workflow.

---

## 2. Generated Artifacts

The migration yields three primary files, structured to separate environment configuration, error handling, and orchestration:

| Target File Path | Language | Role |
| :--- | :--- | :--- |
| `plugins/templates/dw_env_resolver.py` | Python | **Environment & Date Resolver:** Replaces `DW.HOLE_PFAD`. Dynamically calculates relative month windows (`PRELASTMONTH_YYYYMM`, `LASTMONTH_YYYYMM`, `NEXTMONTH_YYYYMM`) and resolves global variables from the Airflow Variable store. |
| `plugins/templates/dw_error_handler.py` | Python | **Error & Log Handler:** Replaces `DW.LESE_LOG`. Acts as an Airflow `on_failure_callback` to capture task failures, emulate legacy `showlog` traces, and trigger failure auditing. |
| `dags/dw_produktion_allgemein_dag.py` | Python | **Master Orchestration DAG:** Preserves the daily midnight schedule (`0 0 * * *`). Integrates the environment resolver, start/end monitoring hooks, and provides the execution skeleton. |

---

## 3. Key Design Decisions

*   **Decoupled Configuration (Airflow Variables):** Legacy hardcoded paths and dynamic `AKTIV_*` toggles are externalized into the Airflow Variable store. This allows administrators to toggle pipeline features (e.g., `AKTIV_CARMEN`) or change bucket paths without modifying code.
*   **Robust Date Calculus:** Replaced error-prone shell date math with Python's standard `datetime` and `dateutil.relativedelta`. Calculations are anchored to the first day of the execution month to prevent calendar overflow bugs (e.g., running on the 31st of a month).
*   **Native Airflow Callbacks:** Instead of checking shell return codes (`$?`) after every step, we utilize Airflow's native `on_failure_callback` mechanism. This guarantees that if any task in the DAG fails, the error handler is immediately invoked with full execution context.
*   **Fail-Fast Strategy:** Retries are set to `0` by default in `DEFAULT_ARGS` to preserve the legacy operational behavior where failures require immediate manual inspection and intervention.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variables Creation
The following variables must be populated in the Cloud Composer Environment (via the Airflow UI **Admin -> Variables** or the `gcloud composer environments run` CLI):

```json
{
  "dwh_home": "gs://<your-production-bucket>/dwh",
  "home": "gs://<your-production-bucket>/home",
  "kws_home": "gs://<your-production-bucket>/kws",
  "pms_home": "gs://<your-production-bucket>/pms",
  "istns_home": "gs://<your-production-bucket>/istns",
  "aktiv_carmen": "0",
  "aktiv_crs": "0",
  "aktiv_ctel": "0",
  "aktiv_dpps": "0",
  "aktiv_kds": "0",
  "aktiv_wuerfel": "0",
  "aktiv_xtra": "0",
  "aktuell_cache_dwk_kkm": "0"
}
```

### 2. IAM & Permissions
Ensure that the Cloud Composer Service Account has:
*   `roles/storage.objectViewer` (or `objectUser`) on the buckets configured in the environment variables.
*   `roles/logging.viewer` if the error handler is expanded to programmatically fetch Stackdriver logs.

### 3. Deployment of Plugins and DAGs
1.  Copy `dw_env_resolver.py` and `dw_error_handler.py` to the Composer environment's `plugins/templates/` directory.
2.  Copy `dw_produktion_allgemein_dag.py` to the Composer environment's `dags/` directory.

---

## 5. Known Gaps & Unresolved References

The following legacy components were flagged as **SOURCE: NOT FOUND** during migration and have been stubbed with clean Python logging placeholders. They must be integrated with your enterprise monitoring systems before final go-live:

1.  **`DW.DWH_ADM_JOB_MONITOR_START` & `DW.DWH_ADM_JOB_MONITOR_END`**
    *   *Gap:* Legacy database-driven audit logging steps.
    *   *Remediation:* Replace the print statements in `register_job_monitor_start`, `register_job_monitor_end`, and `execute_job_monitor_end_failed` with actual database inserts/updates (e.g., using an `Airflow Hook` pointing to your metadata database or BigQuery audit table).
2.  **`SHOWLOG.KSH`**
    *   *Gap:* Legacy binary utility that extracted log files based on UC4 Run IDs.
    *   *Remediation:* The migrated code outputs a Google Cloud Logging search query template. If automated log extraction is required, integrate the Google Cloud Logging Python SDK into `dw_error_handler.py` to fetch and dump the task's container logs.

---

## 6. Validation

### 1. Unit Testing Date Calculus
Run the following Python test to validate that the date boundaries match legacy expectations across month boundaries and leap years:

```python
from templates.dw_env_resolver import calculate_date_windows

# Test Leap Year Transition
res = calculate_date_windows("2024-03-15")
assert res["PRELASTMONTH_YYYYMM"] == "202401"
assert res["LASTMONTH_YYYYMM"] == "202402"
assert res["NEXTMONTH_YYYYMM"] == "202404"

# Test Year Wrap-around
res_jan = calculate_date_windows("2024-01-10")
assert res_jan["PRELASTMONTH_YYYYMM"] == "202311"
assert res_jan["LASTMONTH_YYYYMM"] == "202312"
assert res_jan["NEXTMONTH_YYYYMM"] == "202402"

print("All date calculus unit tests PASSED.")
```

### 2. Integration Testing in Airflow
1.  **Success Path:** Trigger the DAG `dw_produktion_allgemein_includes` manually from the Airflow UI. Verify that all tasks complete with `success` status and that the logs for `initialize_environment` display the correctly resolved paths and dates.
2.  **Failure Path:** 
    *   Temporarily modify `execute_production_work` in the DAG to raise an exception (e.g., replace `EmptyOperator` with a `PythonOperator` that executes `raise ValueError("Simulated Failure")`).
    *   Trigger the DAG.
    *   Verify that the DAG fails immediately and that the task logs show the output from `on_failure_show_log` (including the simulated `showlog` trace and the call to the failed audit monitor).

---

## 7. Rollback Procedure

In the event of an operational failure or unexpected behavior in production:

1.  **Pause the DAG:** Immediately pause the `dw_produktion_allgemein_includes` DAG in the Airflow UI to prevent subsequent scheduled runs.
2.  **Revert Code:** 
    *   Remove the DAG file `dw_produktion_allgemein_dag.py` from the Composer `dags/` folder.
    *   Remove the templates from the `plugins/templates/` folder.
3.  **Legacy Reactivation:** Reactivate the corresponding UC4 workflows on the legacy engine. Because the date calculations are dynamic and derived from the execution date, no manual state alignment is required to resume legacy processing.