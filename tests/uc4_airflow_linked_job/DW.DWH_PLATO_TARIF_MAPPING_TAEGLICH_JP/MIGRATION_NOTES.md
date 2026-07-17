# MIGRATION_NOTES.md

## 1. Summary
This document details the migration of the daily orchestration workflow **DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP** from the legacy UC4 scheduler to Google Cloud Composer (Airflow).

* **Source Platform:** UC4 / Automic Engine
* **Target Platform:** Google Cloud Composer (Airflow) / Google Cloud Platform (GCP)
* **Migration Pattern:** `UC4_ONLY` (Pure Orchestration Workflow)
* **Functional Description:** Coordinates the setup and mapping of Plato-specific tariff datasets to the central Data Warehouse (DWH) base tariffs. It acts as a processing boundary, managing execution states and coordinating downstream resources.

---

## 2. Generated Artifacts
The migration process generated the following files, which are structured to preserve the original repository layout:

### 1. Airflow DAG Wrapper
* **File Path:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_plato_tarif_mapping_taeglich_jp.py`
* **Role:** Defines the Airflow DAG wrapper, task declarations, dependencies, and sync configurations. It maps the legacy UC4 Job Plan (`JOBP`) structure to Airflow operators.

### 2. PySpark Driver Script
* **File Path:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`
* **Role:** Serves as the execution script for the Dataproc task. It replaces the legacy UNIX command task (`JOBS_UNIX`) and executes the exact legacy script footprint.

---

## 3. Key Design Decisions

### Pure Orchestration Pattern (`UC4_ONLY`)
Because this workflow serves as a processing boundary and does not perform direct database transformations, the migration preserves its structural integrity as an orchestration gateway. The legacy UNIX script execution is mapped to a lightweight PySpark execution structure running on a Dataproc cluster.

### Task Folding
The legacy UNIX job `DW.DUMMY_ABSD_PLATO_TARIFE` was folded directly into the parent DAG wrapper as a `DataprocSubmitJobOperator` task (`dw_dwh_dummy_absd_plato_tarife`). This maintains the exact dependency sequence while eliminating the overhead of managing separate task files.

### Concurrency and Serialization Lock
* **Legacy Mechanism:** Sync Object `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC` with rule `Else="Wait"`.
* **Airflow Approach:** Configured `max_active_runs=1` at the DAG level. This natively serializes concurrent DAG runs and prevents overlapping executions, matching the legacy behavior.

### Error Handling and Alerting
* **Legacy Mechanism:** On failure (`ANY_ABEND`), the workflow executes `DW.CALL_STANDARD` with parameters `##911011`.
* **Airflow Approach:** Implemented via `on_failure_callback` pointing to a Python function (`on_failure_alarm`). This function captures execution context telemetry and routes it to operational alerting channels.

### Preservation of Legacy Typo
To maintain strict functional parity and log-matching rules, the legacy print statement typo (`"Doing nothinig"`) is preserved verbatim in the PySpark driver script.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variables Configuration
Ensure the following global variables are declared in the target Cloud Composer environment:

```json
{
  "GCP_PROJECT": "your-gcp-project-id",
  "GCP_REGION": "your-gcp-region",
  "GCS_BUCKET": "your-gcs-bucket-name",
  "DATAPROC_CLUSTER": "your-dataproc-cluster-name"
}
```

### 2. GCS Artifact Deployment
Upload the generated PySpark driver script to the target Cloud Storage bucket:
* **Source:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`
* **Destination:** `gs://<YOUR_GCS_BUCKET>/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py`

### 3. IAM & Permissions
Ensure the Cloud Composer worker service account has the following IAM roles:
* `roles/dataproc.editor` (To submit jobs to the Dataproc cluster)
* `roles/storage.objectViewer` (To read the PySpark script from GCS)

### 4. Scheduling Configuration
The DAG is currently configured with `schedule=None` (Ad-hoc / External Trigger) because no active `JSCH` schedule file was provided. If this workflow must run on a daily schedule, update the DAG parameter:
* **Example:** `schedule="0 2 * * *"` (Daily at 02:00 AM)

---

## 5. Known Gaps & Unresolved References

### 1. Upstream Sync Dependency
The legacy workflow has an upstream sync dependency on `JOB:DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP_SYNC` which is not yet migrated. Currently, concurrency is managed locally via `max_active_runs=1`. Once the upstream sync system is migrated, this should be replaced with an Airflow Dataset trigger or an `ExternalTaskSensor`.

### 2. ENDED_SKIPPED Operational Divergence
In UC4, if a task resolves to `ENDED_SKIPPED`, it does not trigger error escalation. In Airflow, the default `trigger_rule` is set to `ALL_SUCCESS` to safeguard structural lineage. If conditional skipping is introduced upstream in future iterations, a manual review of task trigger rules will be required.

---

## 6. Validation

### 1. Local DAG Parsing Test
Verify that the DAG is syntactically correct and can be parsed by Airflow:
```bash
python3 dags/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_plato_tarif_mapping_taeglich_jp.py
```
*Passing criteria:* The command exits with code `0` without throwing syntax or import errors.

### 2. Execution Run Validation
Trigger a manual run of the DAG in the Airflow UI or via CLI:
```bash
airflow dags trigger dw_dwh_plato_tarif_mapping_taeglich_jp
```

* **Task `start`:** Should complete instantly (Success).
* **Task `dw_dwh_dummy_absd_plato_tarife`:** Should submit the PySpark job to Dataproc.
  * *Log Verification:* Check the Dataproc driver logs for the exact output:
    ```
    Doing nothinig
    ```
* **Task `end`:** Should complete instantly (Success).

---

## 7. Rollback Procedure

In the event of an operational failure or migration rollback:

1. **Pause the Airflow DAG:**
   ```bash
   airflow dags pause dw_dwh_plato_tarif_mapping_taeglich_jp
   ```
2. **Re-enable Legacy UC4 Job Plan:**
   * Log in to the UC4 Automic UI.
   * Locate `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`.
   * Set the Active flag back to `1` (Active).
3. **Verify Legacy Execution:** Ensure the legacy agent on host `DWHDWH1P` resumes orchestration duties.