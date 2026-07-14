# Migration Notes: DW.DWH_SAP_GUTSCHRIFTEN_TAEGLICH_JP

This document details the migration of the legacy Automic/UC4 job orchestration components `DW.HOLE_PFAD` and `DW.LESE_LOG` to Google Cloud Composer (Apache Airflow) on Google Cloud Platform (GCP).

---

## 1. Summary

The migration transitions legacy Automic/UC4 Job Include (`JOBI`) objects into a modular, reusable Python utility and an Airflow DAG. 

* **Source Components:**
  * `DW.HOLE_PFAD` (UC4 Include): Responsible for dynamic path resolution, system activation flag lookups, and relative date calculations (e.g., `LASTMONTH_YYYYMM`).
  * `DW.LESE_LOG` (UC4 Include): Responsible for post-execution return-code evaluation, custom log formatting, and error routing.
* **Target Platform:** Google Cloud Composer (Apache Airflow) running on Google Cloud Platform (GCP).
* **Target Workload:** `DW.DWH_SAP_GUTSCHRIFTEN_TAEGLICH_JP` (Daily SAP Credit Notes processing).

---

## 2. Generated Artifacts

The migration produces two primary files located in the target repository:

1. **`utils/dw_job_helper.py`**
   * **Role:** Reusable utility module. It consolidates the logic of both legacy include files (`DW.HOLE_PFAD` and `DW.LESE_LOG`). It calculates dynamic date boundaries using the Airflow logical execution date and provides standardized post-execution status evaluation.
2. **`dw_sap_gutschriften_taeglich_jp.py`**
   * **Role:** Main Airflow DAG definition file. It orchestrates the initialization task (which pushes calculated variables to XComs) and executes the core workload task, utilizing the helper utility for post-processing.

---

## 3. Key Design Decisions

* **Consolidation of Include Files (`JOBI`):** In UC4, include files are copy-pasted into jobs at runtime. In Airflow, this is replaced by a reusable Python module (`dw_job_helper.py`) imported by DAGs. This avoids code duplication and simplifies maintenance.
* **Idempotency via Logical Date:** Legacy date calculations relied on `SYS_DATE` (the real-time system date). To ensure idempotency during historical backfills, the migrated calculations use Airflow's logical execution date (`context['ds']`).
* **State Sharing via XComs:** Calculated variables (such as paths and date boundaries) are pushed to Airflow XComs during the initialization task, making them dynamically accessible to downstream tasks.
* **Native Logging and Alerting:** The legacy dependency on `SHOWLOG.KSH` is retired. Airflow automatically captures standard output (`stdout`) and standard error (`stderr`), streaming them directly to Google Cloud Logging (Stackdriver). The legacy visual log separators are preserved within the native Airflow task logs for operator familiarity.

---

## 4. Manual Steps Before Go-Live

### Schema & Dataset Creation
* Ensure that any BigQuery datasets or Cloud Storage buckets referenced by downstream scripts (e.g., `/home/dwh/scripts/sap_gutschriften.sh`) are provisioned in the target GCP environment.

### IAM & Permissions
* The Cloud Composer environment's service account must have the following roles:
  * `roles/composer.worker`
  * `roles/logging.logWriter` (to stream task logs to Cloud Logging)
  * Appropriate access roles (e.g., `roles/bigquery.dataEditor`, `roles/storage.objectAdmin`) for resources accessed by downstream workloads.

### Airflow Variables Configuration
The following variables must be configured in the Cloud Composer Airflow UI (**Admin -> Variables**) or via the `gcloud composer environments run` CLI:

| Variable Name | Expected Value / Example | Description |
| :--- | :--- | :--- |
| `dwh_home` | `/home/dwh` | Base path for DWH scripts and configurations |
| `home` | `/home` | Base home directory path |
| `kws_home` | `/home/kws` | Path for KWS subsystem (if applicable) |
| `pms_home` | `/home/pms` | Path for PMS subsystem (if applicable) |
| `istns_home` | `/home/istns` | Path for ISTNS subsystem (if applicable) |
| `aktiv_carmen` | `0` or `1` | Activation flag for CARMEN processing |
| `aktiv_crs` | `0` or `1` | Activation flag for CRS processing |
| `aktiv_ctel` | `0` or `1` | Activation flag for CTEL processing |
| `aktiv_dpps` | `0` or `1` | Activation flag for DPPS processing |
| `aktiv_kds` | `0` or `1` | Activation flag for KDS processing |
| `aktiv_wuerfel` | `0` or `1` | Activation flag for WUERFEL processing |
| `aktiv_xtra` | `0` or `1` | Activation flag for XTRA processing |
| `aktuell_cache` | `some_cache_value` | Cache configuration parameter |

### Scheduling
* The DAG is configured with `schedule_interval="@daily"` and `catchup=False`. Adjust the `start_date` in `DEFAULT_ARGS` to the desired migration go-live date.

---

## 5. Known Gaps & Unresolved References

1. **Redesign (B4) Items — Missing Job Monitors:**
   * The legacy calls to `DW.DWH_ADM_JOB_MONITOR_START` and `DW.DWH_ADM_JOB_MONITOR_END` are omitted. Airflow natively tracks task states (running, success, failed) in its metadata database. If external database logging is required, these must be redesigned as Airflow `on_execute_callback` and `on_success_callback` / `on_failure_callback` functions.
2. **Redundant Logging Tool:**
   * The legacy `SHOWLOG.KSH` script is not migrated. This is a known gap with no target candidate, as Cloud Logging natively replaces its functionality.
3. **Downstream DAG Wiring:**
   * Over 40 downstream data processes (e.g., `DW.DWH_EXIS_ACL_FOS_BONI`) depend on these calculated variables. Because those workloads are not yet migrated, cross-DAG dependencies cannot be fully wired. Downstream DAGs must import `utils.dw_job_helper` individually as they are migrated.

---

## 6. Validation

### How to Run the Tests
1. **Unit Testing:**
   * Run a local Python test to validate `calculate_dwh_variables` date math:
     ```python
     from utils.dw_job_helper import calculate_dwh_variables
     res = calculate_dwh_variables("2026-03-29")
     assert res["LASTMONTH_YYYYMM"] == "202602"
     assert res["PRELASTMONTH_YYYYMM"] == "202601"
     assert res["NEXTMONTH_YYYYMM"] == "202604"
     ```
2. **DAG Dry Run:**
   * Verify the DAG parses without syntax errors using the Airflow CLI:
     ```bash
     airflow dags list-import-errors
     ```
3. **Task Execution Test:**
   * Test individual tasks for a specific historical date:
     ```bash
     airflow tasks test dw_sap_gutschriften_taeglich_jp initialize_legacy_paths 2026-03-29
     airflow tasks test dw_sap_gutschriften_taeglich_jp execute_workload 2026-03-29
     ```

### Meaning of "Passing"
* **`initialize_legacy_paths` passes if:** It successfully writes the correct date strings and Airflow variable values to the task execution log and pushes them to XComs without throwing exceptions.
* **`execute_workload` passes if:** It successfully pulls variables from XComs, executes the mock payload, and outputs the legacy-formatted success block (`Rueckgabewert: '0'`) to the logs, exiting with status code `0`.

---

## 7. Rollback Procedure

In the event of a deployment failure or critical runtime issue:

1. **Pause the Airflow DAG:**
   * Immediately pause the DAG in the Airflow UI or via the CLI to prevent further scheduled executions:
     ```bash
     airflow dags pause dw_sap_gutschriften_taeglich_jp
     ```
2. **Revert Code Deployment:**
   * Roll back the Cloud Storage DAGs folder to the previous stable Git commit:
     ```bash
     git checkout HEAD~1
     gsutil cp dw_sap_gutschriften_taeglich_jp.py gs://<composer-dag-bucket>/dags/
     gsutil cp utils/dw_job_helper.py gs://<composer-dag-bucket>/dags/utils/
     ```
3. **Legacy Reactivation (if fallback is required):**
   * If reverting to the legacy Automic/UC4 environment, reactivate the corresponding UC4 active jobs and verify that the legacy scheduler is enabled for this workflow.