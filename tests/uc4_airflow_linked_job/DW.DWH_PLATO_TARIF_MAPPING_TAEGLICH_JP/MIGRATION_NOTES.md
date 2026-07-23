# Migration Notes: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This document details the migration of the UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Cloud Composer (Apache Airflow).

---

## 1. Summary

The legacy UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated to Cloud Composer (Apache Airflow) as a Python-based DAG. 

* **Source Platform:** UC4 / Automic Engine (UNIX Job)
* **Target Platform:** Cloud Composer (Apache Airflow) / Google Cloud Platform (GCP)
* **Migration Pattern:** `UC4_ONLY` (Pure orchestration migration; no data layer migration is involved)
* **Functional Role:** This job serves as a dummy/placeholder task within the daily mapping workflow sequence (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). It performs no database, system, or file operations, and simply outputs a diagnostic log message.

---

## 2. Generated Artifacts

The migration process generated the following file:

| Target File Path | Role / Description |
| :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` | The production-ready Airflow DAG file. It defines the DAG structure, environment variable lookups, and a `PythonOperator` that executes the dummy logging task. |

---

## 3. Key Design Decisions

### Lightweight Execution (`PythonOperator`)
In the legacy UC4 environment, this job was defined as a `JOBS_UNIX` object executing on the host `DWHDWH1P`. Because its only action is printing a diagnostic message (`Doing nothinig`), migrating this to a heavy compute resource (such as a Dataproc cluster or a dedicated GCE instance) would introduce unnecessary latency and cost. Instead, it is implemented as a native Airflow `PythonOperator` running directly within the Airflow worker namespace.

### Typo Preservation for Parity
The legacy UC4 script printed the message `:print Doing nothinig` (containing a typographical error in "nothinig"). To ensure strict operational parity and prevent breaking any legacy log-scraping, monitoring, or auditing tools that might scan for this exact string, the typo has been preserved verbatim in the Python logging function:
```python
logging.info("Doing nothinig")
```

### Environment Variable Standardization
To maintain structural consistency across all migrated DAGs, standard GCP environment variables (`GCP_PROJECT`, `DATAPROC_REGION`, `DATAPROC_CLUSTER`, and `GCS_BUCKET`) are resolved dynamically at the top of the DAG file using Airflow Variables with OS environment fallbacks. Although these variables are unused by this specific dummy task, keeping them ensures the DAG matches the template and configuration footprint of the rest of the migrated workflow.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling this DAG in production, the following administrative and configuration steps must be completed:

### 1. Airflow Variable Configuration
Ensure that the following Airflow Variables are defined in your Cloud Composer environment (via the Airflow UI under **Admin -> Variables** or via the `gcloud` CLI):
* `GCP_PROJECT`: Your Google Cloud Project ID.
* `DATAPROC_REGION`: The GCP region where Dataproc clusters are deployed.
* `DATAPROC_CLUSTER`: The name of your active Dataproc cluster.
* `GCS_BUCKET`: The primary Cloud Storage bucket used for environment artifacts.

*Note: While this dummy job does not use these variables, they must be present to prevent import-time warnings or failures if your environment enforces strict variable checks.*

### 2. IAM & Permissions
The Cloud Composer Service Account must have the standard `roles/composer.worker` role. No additional BigQuery, Dataproc, or GCS permissions are required for this specific job since it does not interact with GCP data services.

### 3. Scheduling & Integration
Because no `EVNT_TIME` (scheduling) metadata was provided for this sub-component, the DAG is configured with `schedule=None`. 
* **Action Required:** If this job must run on a specific schedule independently of its parent workflow, a cron expression must be manually added to the `schedule` parameter of the DAG. Otherwise, it should remain unscheduled (`None`) and be triggered by the parent workflow.

---

## 5. Known Gaps & Unresolved References

### 1. Missing Parent Workflow Orchestration
The parent UC4 Job Plan `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` has not yet been migrated to Airflow. 
* **Impact:** This DAG currently exists as an isolated, standalone workflow.
* **Resolution (Redesign / B4 Item):** Once the parent Job Plan XML is migrated, this dummy DAG should be refactored. It should either:
  1. Be absorbed directly into the parent DAG as a local `PythonOperator` task node to avoid DAG sprawl.
  2. Be triggered downstream from the parent DAG using a `TriggerDagRunOperator`.

---

## 6. Validation

To validate the migration of this job, perform the following steps:

### Step 1: DAG Import Validation
Deploy the DAG file to your Cloud Composer DAGs bucket:
```bash
gcloud composer environments storage dags import \
    --environment <your-composer-environment> \
    --location <your-gcp-region> \
    --source uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py
```
Verify that no import errors appear in the Airflow UI DAGs list.

### Step 2: Manual Execution
Trigger the DAG manually via the Airflow UI or using the gcloud CLI:
```bash
gcloud composer environments run <your-composer-environment> \
    --location <your-gcp-region> \
    dags trigger -- dw_dwh_dummy_absd_plato_tarife_parent
```

### Step 3: Success Criteria
The validation is considered **passing** if:
1. The DAG run transitions to a `SUCCESS` state.
2. The task `dw_dwh_dummy_absd_plato_tarife` completes successfully with a status of `success`.
3. The task execution logs contain the exact string:
   ```text
   INFO - Doing nothinig
   ```

---

## 7. Rollback Procedure

Because this is a pure orchestration migration (`UC4_ONLY`) with no stateful data modifications, rollback is low-risk and straightforward:

1. **Pause the Airflow DAG:** Navigate to the Airflow UI and toggle the DAG `dw_dwh_dummy_absd_plato_tarife_parent` to **Off** (paused).
2. **Remove the Artifact:** Delete the DAG file from the Cloud Composer GCS bucket:
   ```bash
   gsutil rm gs://<your-composer-dag-bucket>/dags/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py
   ```
3. **Re-enable Legacy Job:** If the legacy UC4 engine is still active, ensure that the UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is active (`<Active>1</Active>`) and enabled within the UC4 scheduler.