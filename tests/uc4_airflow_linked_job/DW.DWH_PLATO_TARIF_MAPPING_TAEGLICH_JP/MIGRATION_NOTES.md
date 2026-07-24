# MIGRATION NOTES: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

## 1. Summary
The legacy UC4 (Automic) Unix job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated to Google Cloud Composer (Apache Airflow). 

In the legacy environment, this job functioned as a synchronization milestone or structural placeholder within the daily Plato Tariff mapping workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`). It executed a simple no-op command (`:print Doing nothinig`). In the target Cloud Composer platform, this has been translated into a lightweight, native Airflow DAG that logs the identical message to preserve execution lineage and milestone tracking without incurring unnecessary cloud compute overhead.

---

## 2. Generated Artifacts
The migration process generated the following file:

* **Target File:** `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py`
* **Role:** An Airflow DAG file defining a single-task workflow. It utilizes a `PythonOperator` to execute a local logging function, mimicking the legacy dummy execution step.

---

## 3. Key Design Decisions

### Native PythonOperator vs. Dataproc Submit Operator
* **Decision:** The initial migration design suggested wrapping the dummy script in a PySpark file and executing it via `DataprocSubmitJobOperator` to maintain architectural consistency with other heavy data-processing jobs. However, spinning up or submitting a job to a Dataproc cluster solely to print a log line is highly inefficient and costly. 
* **Refinement:** The design was optimized to use a native Airflow `PythonOperator`. This executes in-memory on the Cloud Composer worker, eliminating Dataproc cluster latency and run costs while achieving the exact same functional outcome.

### Output Literal Preservation
* **Decision:** To comply with strict output validation rules and support any legacy log-scraping or monitoring tools, the original legacy typo `"Doing nothinig"` has been preserved character-for-character in the Python logging statement.

### Environment-Specific Variables
* **Decision:** Global variables such as `GCP_PROJECT` and `GCP_REGION` are retrieved dynamically at runtime using Airflow's `Variable.get()` interface. This prevents hardcoded environment values and ensures the DAG file is promotion-ready across Development, UAT, and Production environments without modification.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling this DAG in production, the following manual setup steps must be completed:

### 1. Airflow Variables Configuration
Ensure that the following Airflow variables are defined in the target Cloud Composer environment:
* `GCP_PROJECT`: The Google Cloud Project ID where the environment runs.
* `GCP_REGION`: The default GCP region (e.g., `europe-west3`).

### 2. IAM & Permissions
Since this DAG runs locally within the Cloud Composer worker and does not interact with external GCP resources (like BigQuery or Dataproc), no special external IAM permissions are required beyond standard Composer worker execution rights.

### 3. Scheduling & Parent Integration
* The DAG is currently configured with `schedule=None` because no scheduler (`JSCH`) or event (`EVNT_TIME`) file was provided in the source package.
* **Crucial Step:** This job is designed to run as part of the parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`. Once the parent workflow is migrated, this DAG must either:
  1. Be integrated directly as a task node inside the parent DAG.
  2. Be triggered programmatically from the parent DAG using a `TriggerDagRunOperator`.

---

## 5. Known Gaps & Unresolved References

* **Unmigrated Parent Workflow:** The parent container `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.xml` has not yet been migrated. Consequently, this job currently exists as a standalone DAG. Full end-to-end integration testing cannot be completed until the parent workflow is migrated and deployed.
* **Missing Scheduler Context:** Because no scheduling metadata was provided, the DAG cannot run on a time-based trigger in its current state. It must be triggered manually or via external orchestration.

---

## 6. Validation

To validate the migrated DAG, perform the following steps:

### Local/CLI Validation
Run an Airflow task test command within your Cloud Composer environment or local development kit:

```bash
airflow dags test dw_dwh_dummy_absd_plato_tarife
```

Alternatively, test the specific task:

```bash
airflow tasks test dw_dwh_dummy_absd_plato_tarife dw_dwh_dummy_absd_plato_tarife 2026-03-30
```

### What "Passing" Looks Like
1. The command execution returns an exit code of `0` (Success).
2. The task log output contains the exact string:
   ```text
   INFO - Doing nothinig
   ```

---

## 7. Rollback Procedure

If issues arise post-deployment, execute the following steps to roll back:

1. **Pause the Airflow DAG:**
   Go to the Airflow UI and toggle the DAG `dw_dwh_dummy_absd_plato_tarife` to **Off** (Paused).
   
2. **Remove the DAG File:**
   Delete the generated Python file from the Cloud Composer DAGs bucket:
   ```bash
   gcloud storage rm gs://<your-composer-dags-bucket>/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py
   ```

3. **Re-enable UC4 Job:**
   In the legacy UC4 system, locate the `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` object, ensure its active flag is set to `1` (Active), and verify it is correctly linked back into the active parent Job Plan.