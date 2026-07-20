# MIGRATION NOTES: DW.DWH_ABRECHNUNG_REFORMAT_JP

This document provides the migration details, design decisions, manual setup steps, known gaps, and validation procedures for migrating the UC4 Job Plan `DW.DWH_ABRECHNUNG_REFORMAT_JP` to Google Cloud Composer (Airflow).

---

## 1. Summary
The **DW.DWH_ABRECHNUNG_REFORMAT_JP** workflow has been migrated from a legacy UC4 Job Plan (`JOBP`) to a Google Cloud Composer (Airflow) DAG. 

* **Source Platform:** UC4 (Automic) Engine
* **Target Platform:** Google Cloud Composer (Airflow 2.x) / Google Cloud Platform (GCP)
* **Purpose:** This workflow orchestrates the daily reformatting of billing and settlement data (*Abrechnungsdaten*) for downstream consumption. It acts as an orchestration wrapper that monitors multiple upstream data pipelines and triggers the core reformatting execution logic.

---

## 2. Generated Artifacts
The migration process generated the following target file:

| Target File Path | Role | Description |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_ABRECHNUNG/DW_DWH_ABRECHNUNG_REFORMAT_JP.py` | Orchestrator DAG | The primary Airflow DAG file containing the workflow structure, upstream sensors, and the child DAG trigger. |

---

## 3. Key Design Decisions

### Decoupling of Orchestration and Execution
* **Decision:** The original UC4 Job Plan (`JOBP`) was strictly an orchestration component, while its child task `DW.DWH_ABRECHNUNG_REFORMAT_JS` contained the actual execution logic (historically a Perl script). To prevent code duplication and maintain clean separation of concerns, the migrated DAG does not execute Dataproc or PySpark jobs directly. Instead, it uses the `TriggerDagRunOperator` to trigger the child DAG (`dw_dwh_abrechnung_reformat_js`) and waits for its completion (`wait_for_completion=True`).

### Cross-DAG Dependency Management
* **Decision:** The workflow depends on four external processes that run on varying schedules (weekly, daily, monthly). These dependencies are modeled using `ExternalTaskSensor` operators:
  1. `sensor_kunde_abgl_woechentlich` (Weekly)
  2. `sensor_rechnung_export_taeglich` (Daily)
  3. `sensor_tarifhist_scd_monatlich` (Monthly)
  4. `sensor_umsatz_konsolidierung_monatlich` (Monthly)
* **Trade-off:** Using sensors can consume Airflow worker slots. To mitigate this, all sensors are configured with `mode='reschedule'`, which releases the worker slot between pokes (configured at 5-minute intervals).

### Concurrency and Overlap Prevention
* **Decision:** To replicate the UC4 synchronization behavior and prevent concurrent executions of this daily pipeline, `max_active_runs` is set to `1` at the DAG level.

### Folder Integrity and Metadata Preservation
* **Decision:** The target file is placed in the exact mirrored directory structure (`DWH/DWH_KERN/PRODUKTION/DW.DWH_ABRECHNUNG/`) to maintain repository integrity. The original German description from the UC4 XML has been preserved verbatim in the DAG's description attribute.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling this DAG in production, the following manual setup steps must be completed:

### 1. Airflow Variables Configuration
Ensure the following global Airflow variables are defined in your Cloud Composer environment:
* `GCP_PROJECT`: The GCP Project ID where resources are located.
* `GCP_REGION`: The GCP region (e.g., `europe-west3`).
* `GCS_BUCKET`: The GCS bucket used for environment-wide storage.

### 2. IAM & Permissions
* The Cloud Composer Service Account must have the **Composer User** (`roles/composer.user`) or equivalent Airflow RBAC permissions to trigger other DAG runs via the `TriggerDagRunOperator`.

### 3. Upstream DAG Registration
The four upstream DAGs monitored by the sensors must be deployed and active in the Composer environment:
* `dw_dwh_kunde_abgl_woechentlich_js`
* `dw_dwh_rechnung_export_taeglich_js`
* `dw_dwh_tarifhist_scd_monatlich_js`
* `dw_dwh_umsatz_konsolidierung_monatlich_js`

### 4. Child DAG Registration
The child execution DAG (`dw_dwh_abrechnung_reformat_js`) must be deployed and active.

### 5. Scheduling & Calendar Alignment
* The current schedule is set to a daily placeholder of `02:00 AM` (`0 2 * * *`). Verify this execution window against the business requirements and legacy UC4 calendar definitions.

---

## 5. Known Gaps & Unresolved References

### Unmigrated Upstream Dependencies (Redesign B4 Items)
The following upstream DAGs referenced by the `ExternalTaskSensor` operators are not yet migrated:
* `dw_dwh_kunde_abgl_woechentlich_js`
* `dw_dwh_rechnung_export_taeglich_js`
* `dw_dwh_tarifhist_scd_monatlich_js`
* `dw_dwh_umsatz_konsolidierung_monatlich_js`

> **Warning:** Enabling this DAG before these upstream DAGs are deployed will cause the sensors to time out and fail the workflow.

### Sensor Execution Delta Alignment
* The sensors currently use `execution_delta=timedelta(hours=0)`. Because the upstream jobs run on different schedules (weekly, monthly), their execution dates may not align exactly with this daily DAG's execution date. 
* **Follow-up Action:** Adjust the `execution_delta` or implement an `execution_date_fn` on the sensors to map the daily run to the correct weekly/monthly execution runs of the upstream DAGs.

---

## 6. Validation

To validate the migrated DAG, perform the following tests:

### 1. Static Analysis & Compilation Test
Verify that the DAG compiles without syntax or import errors:
```bash
python3 -m py_compile DWH/DWH_KERN/PRODUKTION/DW.DWH_ABRECHNUNG/DW_DWH_ABRECHNUNG_REFORMAT_JP.py
```

### 2. Airflow DAG Bag Validation
Verify that Airflow can successfully load the DAG into its `DagBag`:
```python
from airflow.models import DagBag
dagbag = DagBag(dag_folder='DWH/DWH_KERN/PRODUKTION/DW.DWH_ABRECHNUNG/')
assert 'dw_dwh_abrechnung_reformat_jp' in dagbag.dags
assert len(dagbag.import_errors) == 0
```

### 3. Task-Level Dry Run
Test individual tasks (such as the trigger operator) using the Airflow CLI:
```bash
airflow tasks test dw_dwh_abrechnung_reformat_jp trigger_dw_dwh_abrechnung_reformat_js 2026-01-01
```

### Definition of "Passing"
* The DAG loads in the Airflow UI with zero import errors.
* The dependency graph matches the design: 4 sensors pointing to `start`, which points to `trigger_dw_dwh_abrechnung_reformat_js`, which points to `end`.
* Triggering the DAG manually successfully pokes the sensors and triggers the child DAG when upstreams are satisfied.

---

## 7. Rollback Procedure

If issues are encountered during or after deployment, follow these steps to roll back:

1. **Pause the Airflow DAG:**
   Disable the DAG in the Airflow UI or via the CLI to prevent further scheduled runs:
   ```bash
   airflow dags pause dw_dwh_abrechnung_reformat_jp
   ```
2. **Reactivate the UC4 Job Plan:**
   Re-enable the legacy `DW.DWH_ABRECHNUNG_REFORMAT_JP` Job Plan in the UC4 environment.
3. **Remove Target Artifact (Optional):**
   If a clean environment rollback is required, delete the DAG file from the Composer GCS bucket:
   ```bash
   gsutil rm gs://<your-composer-bucket>/dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_ABRECHNUNG/DW_DWH_ABRECHNUNG_REFORMAT_JP.py
   ```