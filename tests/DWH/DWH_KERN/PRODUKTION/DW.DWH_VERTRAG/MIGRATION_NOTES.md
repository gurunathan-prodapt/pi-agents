# Migration Notes

## 1. Summary
This document details the migration of the weekly contract and tariff synchronization workflow (`DW.DWH_VERTRAG_TARIF_SYNC_JP.xml`) from the legacy **UC4 / Automic** scheduler to **Google Cloud Platform (GCP)**. 

The target platform utilizes **Cloud Composer (Apache Airflow)** for orchestration and **BigQuery** for data warehousing. The migration converts legacy XML job plans, job scripts, and shared include files (`JOBI` objects) into a modular, Python-based Airflow DAG and associated task modules.

---

## 2. Generated Artifacts
The following files have been generated to replace the legacy UC4 components:

| Target File Path | Role / Purpose |
| :--- | :--- |
| `dags/dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py` | **DAG Orchestrator:** Defines the workflow structure, scheduling (`0 3 * * 7`), and task dependencies. |
| `dags/dwh_vertrag/tasks/dw_dwh_vertrag_tarif_sync_start.py` | **Start Task Logic:** Validates the execution lock state (`SYNC_STATUS`), updates state variables, and initializes the run. |
| `dags/dwh_vertrag/tasks/dw_dwh_vertrag_tarif_sync_ende.py` | **End Task Logic:** Resets the execution lock state to `FREI` and logs successful completion. |
| `dags/dwh_vertrag/includes/dw_hole_pfad_vtrg.py` | **Shared Helper (Paths):** Replaces the legacy `:inc DW.HOLE_PFAD_VTRG` include to fetch environment paths. |
| `dags/dwh_vertrag/includes/dw_lese_log_vtrg.py` | **Shared Helper (Logging):** Replaces the legacy `:inc DW.LESE_LOG_VTRG` include to write standardized execution logs. |

---

## 3. Key Design Decisions

### Modularization of Shared Includes
In UC4, shared code blocks are injected inline via `:inc` commands. To maintain clean, maintainable, and dry Python code, these includes were migrated to standalone Python modules under `dags/dwh_vertrag/includes/`. They are imported dynamically by the task modules, preserving the single-responsibility principle.

### State Management & Locking Mechanism
The legacy workflow relies on persistent variables (`DW.VARIABLEN_VTRG`) to prevent concurrent executions and track the last successful run date. 
* **Decision:** Airflow Variables (`Variable.get` / `Variable.set`) are used to store and mutate `DW_VARIABLEN_VTRG_SYNC_STATUS` and `DW_VARIABLEN_VTRG_LETZTER_LAUF`.
* **Trade-off:** While Airflow Variables are easy to implement, they are not strictly transactional. For high-concurrency environments, a BigQuery metadata state table is preferred. However, because this job is scheduled weekly with `max_active_runs=1`, Airflow Variables provide a lightweight and reliable solution without extra database overhead.

### Conformance to Output/Print Literal Rule
To ensure operational continuity and compatibility with existing log-parsing tools, all German log messages and console outputs have been preserved verbatim (e.g., `"Vertrags-/Tarifabgleich fuer {lauf_datum} ist gesperrt - Abbruch"`).

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variables Configuration
Before running the DAG, the following Airflow Variables must be configured in the Cloud Composer environment (via the Airflow UI under **Admin -> Variables** or the `gcloud composer` CLI):

```json
{
  "GCP_DWH_HOME": "/opt/dwh",
  "GCP_HOME": "/home/dwh_user",
  "GCP_PMS_HOME": "/opt/pms",
  "DW_VARIABLEN_VTRG_SYNC_STATUS": "FREI",
  "DW_VARIABLEN_VTRG_LETZTER_LAUF": "19700101"
}
```

### 2. IAM & Permissions
Ensure that the Cloud Composer Service Account has the following permissions:
* **Storage Object Viewer** (`roles/storage.objectViewer`) on the environment's GCS bucket.
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) and **BigQuery Job User** (`roles/bigquery.jobUser`) if downstream tasks interact with BigQuery datasets.

### 3. Scheduling & Deployment
1. Copy the generated files to your Cloud Composer DAGs bucket:
   ```bash
   gsutil cp -r dags/* gs://<your-composer-bucket>/dags/
   ```
2. Verify that the DAG is parsed successfully in the Airflow UI without import errors.

---

## 5. Known Gaps & Unresolved References
* **Downstream Sync Logic:** The legacy job plan orchestrates a synchronization between `STAMMDATEN` and `DWH_KERN`. The current migration covers the start/end lifecycle and state locking. Any actual data transfer scripts (e.g., SQL scripts or Bash commands) that run between the start and end tasks must be integrated as intermediate tasks in `dw_dwh_vertrag_tarif_sync_jp.py`.
* **Redesign (B4) Items:** If transactional safety for the lock state becomes an issue due to manual operator interventions, it is recommended to migrate the state tracking from Airflow Variables to a dedicated BigQuery metadata table (`metadata.job_state`).

---

## 6. Validation

### Local/Development Testing
To validate the task execution flow and state transitions, run the tasks locally using the Airflow CLI:

1. **Test Start Task (Successful Run):**
   Ensure `DW_VARIABLEN_VTRG_SYNC_STATUS` is set to `FREI`.
   ```bash
   airflow tasks test dw_dwh_vertrag_tarif_sync_jp dw_dwh_vertrag_tarif_sync_start_js 2026-07-16
   ```
   * **Expected Outcome:** Task completes successfully. Variable `DW_VARIABLEN_VTRG_SYNC_STATUS` changes to `LAEUFT`.

2. **Test Start Task (Locked Run):**
   Run the start task again while the status is `LAEUFT` or manually set it to `GESPERRT`.
   ```bash
   airflow tasks test dw_dwh_vertrag_tarif_sync_jp dw_dwh_vertrag_tarif_sync_start_js 2026-07-16
   ```
   * **Expected Outcome:** Task fails with `AirflowFailException` and logs:  
     `"Vertrags-/Tarifabgleich fuer <Datum> ist gesperrt - Abbruch"`

3. **Test End Task:**
   ```bash
   airflow tasks test dw_dwh_vertrag_tarif_sync_jp dw_dwh_vertrag_tarif_sync_ende_js 2026-07-16
   ```
   * **Expected Outcome:** Task completes successfully. Variable `DW_VARIABLEN_VTRG_SYNC_STATUS` resets to `FREI`. Logs output:  
     `"Vertrags-/Tarifabgleich fuer Lauf <Datum> erfolgreich beendet"`

---

## 7. Rollback Procedure
In the event of a deployment failure or critical issue:

1. **Pause the DAG:** Immediately pause the DAG in the Airflow UI or via CLI:
   ```bash
   gcloud composer environments run <env-name> \
       --location <region> \
       dags pause -- dw_dwh_vertrag_tarif_sync_jp
   ```
2. **Reset State Variables:** Reset the lock variable to prevent blocking other systems:
   ```bash
   gcloud composer environments run <env-name> \
       --location <region> \
       variables set -- DW_VARIABLEN_VTRG_SYNC_STATUS FREI
   ```
3. **Remove/Revert Artifacts:** Delete the migrated files from the GCS DAGs bucket or redeploy the previous stable version from your Git repository:
   ```bash
   gsutil rm -r gs://<your-composer-bucket>/dags/dwh_vertrag/
   ```