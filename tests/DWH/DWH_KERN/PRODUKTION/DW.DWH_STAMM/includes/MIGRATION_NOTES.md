# MIGRATION_NOTES.md

## 1. Summary
This migration covers the transition of shared utility includes from the legacy UC4 scheduler to Google Cloud Composer (Apache Airflow). 

* **Source Path:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes`
* **Target Platform:** Google Cloud Composer (Apache Airflow) / Google Cloud Platform (GCP)
* **Migration Pattern:** Pure orchestration migration (`UC4_ONLY`). 

Because UC4 Includes (`JOBI` objects) do not represent standalone scheduled execution graphs, they have been refactored into reusable, modular Python helper modules. These modules preserve the original directory structure and logic, making them available for downstream DAGs to import and execute.

---

## 2. Generated Artifacts
The following files were generated to replace the legacy UC4 XML includes:

| Target File Path | Role / Purpose |
| :--- | :--- |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.py` | **Configuration Path Resolver:** A Python module that retrieves environment path variables from the Airflow Variable Metadata DB and pushes them to XCom for downstream tasks. |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.py` | **Execution Logger:** A Python logging helper that extracts runtime DAG and task metadata from the Airflow context and writes a standardized log entry. |

---

## 3. Key Design Decisions
* **Modular Python Helpers instead of Standalone DAGs:** Since `JOBI` objects are reusable code snippets rather than independent workflows, they are compiled as standard Python modules. This avoids polluting the Airflow DAG list with non-runnable DAGs while preserving folder structure integrity.
* **Airflow Variables for Global Configuration:** Legacy UC4 variable container lookups (`GET_VAR` against `DW.VARIABLEN`) are mapped to Airflow Variables (`Variable.get()`). This centralizes environment configuration management.
* **XCom for Downstream Propagation:** `DW.HOLE_PFAD_KNZB.py` pushes resolved paths to XCom (`DWH_HOME`, `HOME`, `ISTNS_HOME`). This allows any downstream task in a calling DAG to dynamically pull and use these paths.
* **Preservation of German Logging Formats:** To ensure compatibility with legacy log-parsing scripts and maintain operational continuity, the exact German log output format (`Protokolleintrag: {task_id} innerhalb {dag_id}`) has been preserved in `DW.LESE_LOG_KNZB.py`.
* **Context-Safe Fallbacks:** Both modules are designed with safe fallbacks (e.g., checking if `context` or `ti` is present) so they can be imported, unit-tested, or executed outside of an active Airflow task instance without throwing `KeyError` or `AttributeError`.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variable Setup
The following variables must be provisioned in the Cloud Composer Airflow Metadata Database (via the Airflow UI **Admin -> Variables** or the `gcloud composer environments run` CLI):

| Variable Key | Expected Value Example | Description |
| :--- | :--- | :--- |
| `dw_variablen_dwh_home` | `gs://<your-gcs-bucket>/dwh_home` | Root path for DWH home directory |
| `dw_variablen_home` | `gs://<your-gcs-bucket>/home` | Root path for user/system home directory |
| `dw_variablen_istns_home` | `gs://<your-gcs-bucket>/istns_home` | Root path for ISTNS home directory |

### 2. IAM & Permissions
Ensure that the Cloud Composer Service Account has the necessary IAM roles to read variables and write logs:
* **Storage Object Viewer** (`roles/storage.objectViewer`) on the GCS buckets referenced by the variables.
* **Composer Worker** (`roles/composer.worker`) to execute the tasks and write logs to Cloud Logging.

### 3. Downstream DAG Integration
Since these are shared includes, they must be imported and called within your migrated parent DAGs. 
* **Example Import:**
  ```python
  from DWH.DWH_KERN.PRODUKTION.DW.DWH_STAMM.includes.DW.HOLE_PFAD_KNZB import get_path_variables
  from DWH.DWH_KERN.PRODUKTION.DW.DWH_STAMM.includes.DW.LESE_LOG_KNZB import write_execution_log
  ```
* **Example Task Definition:**
  ```python
  task_load_config = PythonOperator(
      task_id="hole_pfad_knzb_resolve",
      python_callable=get_path_variables,
      provide_context=True
  )
  ```

---

## 5. Known Gaps & Unresolved References
* **Missing Parent Workflows (B4 Redesign Items):**
  The downstream consumer Job Plans (`JOBP`/`JSCH`) have not yet been migrated:
  * `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS`
  * `DW.DWH_STAMM_KNZB_ABGL_START_JS`
  
  **Action Required:** Once these parent workflows are migrated to Airflow DAGs, developers must manually wire `DW.HOLE_PFAD_KNZB.py` and `DW.LESE_LOG_KNZB.py` into their task execution chains (e.g., as pre-execute hooks or initial setup tasks).

---

## 6. Validation

### Unit Testing
You can validate the Python modules locally or within a CI/CD pipeline without running a full Airflow scheduler.

1. **Test Path Resolution (`DW.HOLE_PFAD_KNZB.py`):**
   Mock the Airflow `Variable` model and verify that the correct dictionary is returned:
   ```python
   from unittest.mock import patch
   from DWH.DWH_KERN.PRODUKTION.DW.DWH_STAMM.includes.DW.HOLE_PFAD_KNZB import get_path_variables

   @patch('airflow.models.Variable.get')
   def test_get_path_variables(mock_get):
       mock_get.side_effect = lambda key: f"mocked_{key}"
       result = get_path_variables()
       assert result["DWH_HOME"] == "mocked_dw_variablen_dwh_home"
       assert result["HOME"] == "mocked_dw_variablen_home"
       assert result["ISTNS_HOME"] == "mocked_dw_variablen_istns_home"
   ```

2. **Test Logging (`DW.LESE_LOG_KNZB.py`):**
   Verify that the log output matches the legacy format:
   ```python
   from DWH.DWH_KERN.PRODUKTION.DW.DWH_STAMM.includes.DW.LESE_LOG_KNZB import write_execution_log

   def test_write_execution_log(caplog):
       import logging
       with caplog.at_level(logging.INFO):
           write_execution_log(context={
               'dag': type('MockDAG', (object,), {'dag_id': 'TEST_DAG'}),
               'task_instance': type('MockTI', (object,), {'task_id': 'TEST_TASK'})
           })
       assert "Protokolleintrag: TEST_TASK innerhalb TEST_DAG" in caplog.text
   ```

### What "Passing" Means
* **`get_path_variables`** successfully queries the Airflow metadata database, returns a dictionary with the three path keys, and pushes them to XCom when run inside a task instance.
* **`write_execution_log`** successfully writes a log line containing the active task ID and DAG ID in the exact format: `Protokolleintrag: <task_id> innerhalb <dag_id>`.

---

## 7. Rollback Procedure
Because these files are stateless Python utility modules, rolling back is straightforward:

1. **Code Reversion:** Revert any imports or task references to these modules in your parent DAGs.
2. **File Deletion (Optional):** Remove the generated `.py` files from the Cloud Composer DAGs bucket:
   ```bash
   gcloud storage rm gs://<composer-dag-bucket>/dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.py
   gcloud storage rm gs://<composer-dag-bucket>/dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.py
   ```
3. **Variable Cleanup (Optional):** If no other workflows require them, delete the variables from the Airflow Metadata DB:
   ```bash
   gcloud composer environments run <env-name> \
       --location <location> \
       variables destroy -- dw_variablen_dwh_home dw_variablen_home dw_variablen_istns_home
   ```