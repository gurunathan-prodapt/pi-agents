# MIGRATION_NOTES.md

**Job:** Shared Files — `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes`  
**Target Platform:** Google Cloud BigQuery & Cloud Composer (Apache Airflow)

---

## 1. Summary

This migration covers the shared include files (originally UC4/Automic JOBI/XML configurations) for the `DW.DWH_VERTRAG` workflow. These files have been refactored into reusable Python modules and helper DAGs for Cloud Composer (Apache Airflow).

* **Source Platform:** UC4 / Automic Engine (XML-based Job Includes)
* **Target Platform:** Cloud Composer (Apache Airflow) & Google Cloud Storage (GCS)
* **Migrated Components:**
  * `DW.HOLE_PFAD_VTRG.xml` $\rightarrow$ `dags/includes/dw_hole_pfad_vtrg.py` (Shared environment variable loader)
  * `DW.LESE_LOG_VTRG.xml` $\rightarrow$ `dags/includes/dw_lese_log_vtrg.py` (Shared execution context logger)

---

## 2. Generated Artifacts

The migration process generated the following target files:

### 1. `dags/includes/dw_hole_pfad_vtrg.py`
* **Role:** A reusable Python module designed to load global environment paths (such as DWH home, user home, and PMS home directories) from Airflow's metadata store.
* **Key Function:** `load_dw_variables()` parses a unified JSON-based Airflow Variable (`dw_variablen`) and returns a validated dictionary of paths.

### 2. `dags/includes/dw_lese_log_vtrg.py`
* **Role:** A dual-purpose logging utility. It contains:
  * A reusable core function (`log_vtrg_context_executable`) that can be imported directly into downstream DAGs to log execution context.
  * A standalone helper DAG (`dw_lese_log_vtrg_helper`) containing a `PythonOperator` task to verify or execute the logging logic independently.

---

## 3. Key Design Decisions

### Unified JSON Variable Container
* **Decision:** Instead of creating separate Airflow Variables for every individual path (which increases database round-trips and clutter), a single JSON-based Airflow Variable named `dw_variablen` was implemented.
* **Trade-off:** Requires downstream developers to maintain a structured JSON object in the Airflow UI/CLI, but significantly reduces metastore overhead and simplifies variable packaging.

### Context Resolution via Native Airflow Jinja Macros
* **Decision:** The legacy UC4 parameters `&ADMJP` (parent workflow) and `&ADMJOB` (current task) are mapped directly to Airflow's native task context keys: `context['dag'].dag_id` and `context['task'].task_id`.
* **Reasoning:** This eliminates the need for custom parameter-passing logic and leverages Airflow's native runtime tracking.

### Dual-Purpose Logging Architecture
* **Decision:** `dw_lese_log_vtrg.py` was structured to export a clean Python function *and* define a standalone helper DAG.
* **Reasoning:** This allows downstream DAGs to import the function directly for inline logging, while still providing an isolated DAG for testing and ad-hoc execution.

---

## 4. Manual Steps Before Go-Live

Before deploying any downstream workflows that import these includes, the following configuration steps must be completed in the target Cloud Composer environment:

### 1. Airflow Variable Creation
You must create the `dw_variablen` JSON variable. 

* **Via Airflow UI:** Navigate to **Admin -> Variables**, click **Add a new record**, and configure:
  * **Key:** `dw_variablen`
  * **Val (JSON):**
    ```json
    {
      "dwh_home": "gs://<your-environment-bucket>/dwh",
      "home": "/home/airflow",
      "pms_home": "gs://<your-environment-bucket>/pms"
    }
    ```
* **Via gcloud CLI:**
  ```bash
  gcloud composer environments run <composer-env-name> \
    --location <location> \
    variables set -- dw_variablen '{"dwh_home": "gs://<your-bucket>/dwh", "home": "/home/airflow", "pms_home": "gs://<your-bucket>/pms"}'
  ```

### 2. IAM & Permissions
Ensure that the Cloud Composer Environment Service Account (typically the default Compute Engine service account or a custom fine-grained Google Service Account) has read/write permissions (`roles/storage.objectViewer` or `roles/storage.objectAdmin`) on the GCS buckets specified in the `dw_variablen` JSON.

### 3. Deployment Directory
Copy the generated files to your Cloud Composer DAGs bucket under the `includes/` subdirectory:
* `gs://<composer-dag-bucket>/dags/includes/dw_hole_pfad_vtrg.py`
* `gs://<composer-dag-bucket>/dags/includes/dw_lese_log_vtrg.py`

---

## 5. Known Gaps & Unresolved References

### Unmigrated Downstream Consumers
The following downstream workflows are referenced in the legacy system but have not yet been migrated to Cloud Composer:
* `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS`
* `DW.DWH_VERTRAG_TARIF_SYNC_START_JS`

**Action Item:** Once these downstream DAGs are migrated, they must be configured to import the helper modules. Example import syntax:
```python
from includes.dw_hole_pfad_vtrg import load_dw_variables
from includes.dw_lese_log_vtrg import log_vtrg_context_executable
```

---

## 6. Validation

To validate that the migrated includes are functioning correctly:

### 1. Syntax and Import Validation
Run a local Python compilation check on the files to ensure there are no syntax errors or missing dependencies:
```bash
python3 -m py_compile dags/includes/dw_hole_pfad_vtrg.py
python3 -m py_compile dags/includes/dw_lese_log_vtrg.py
```

### 2. Helper DAG Execution (Validation of `dw_lese_log_vtrg`)
1. Trigger the `dw_lese_log_vtrg_helper` DAG manually from the Airflow UI or CLI:
   ```bash
   gcloud composer environments run <composer-env-name> \
     --location <location> \
     dags trigger -- dw_lese_log_vtrg_helper
   ```
2. Verify that the task `log_vtrg_context` completes with a status of `SUCCESS`.
3. Inspect the task logs. You should see a German log entry formatted as follows:
   ```text
   INFO - Protokolleintrag: log_vtrg_context innerhalb dw_lese_log_vtrg_helper
   ```

### 3. Variable Loader Validation (Validation of `dw_hole_pfad_vtrg`)
You can verify the variable loader by running a quick test script inside the Airflow Python environment (e.g., via a temporary test DAG or an interactive shell in the worker pod):
```python
from includes.dw_hole_pfad_vtrg import load_dw_variables

try:
    paths = load_dw_variables()
    print("Validation Passed! Loaded paths:", paths)
except Exception as e:
    print("Validation Failed:", str(e))
```
* **Passing Criteria:** The function successfully returns a dictionary containing `DWH_HOME`, `HOME`, and `PMS_HOME` with their configured GCS/local paths.

---

## 7. Rollback Procedure

If issues arise during deployment or execution, follow these steps to roll back:

1. **Delete the Include Files from GCS:**
   Remove the files from the Cloud Composer DAGs bucket to prevent downstream imports of broken code:
   ```bash
   gsutil rm gs://<composer-dag-bucket>/dags/includes/dw_hole_pfad_vtrg.py
   gsutil rm gs://<composer-dag-bucket>/dags/includes/dw_lese_log_vtrg.py
   ```
2. **Delete the Helper DAG:**
   Ensure the helper DAG is removed from the Airflow Web UI.
3. **Remove/Revert Airflow Variables:**
   If necessary, delete or revert the `dw_variablen` Airflow Variable:
   ```bash
   gcloud composer environments run <composer-env-name> \
     --location <location> \
     variables delete -- dw_variablen
   ```