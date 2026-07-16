# Migration Notes: Shared Files — DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes

## 1. Summary
This document details the migration of the shared UC4 Job Include (`JOBI`) scripts belonging to the `DW.DWH_VERTRAG` includes namespace. 

* **Source Platform:** UC4/Automic Workload Automation (Job Include `JOBI` objects).
* **Target Platform:** Google Cloud Composer (Apache Airflow) running on Google Cloud Platform (GCP).
* **Migrated Components:**
  * `DW.HOLE_PFAD_VTRG` (Dynamic path resolution helper).
  * `DW.LESE_LOG_VTRG` (Standardized execution logging helper).

These components have been refactored from inline UC4 XML script fragments into reusable, modular Python utilities designed to be imported and executed by downstream Airflow DAGs.

---

## 2. Generated Artifacts
The migration process generated two core Python modules, which should be deployed to the shared utilities directory of the Cloud Composer environment:

| Target File Path | Component Role | Description |
| :--- | :--- | :--- |
| `dags/utils/path_resolver.py` | Dynamic Path Resolver | A Python class (`DynamicPathResolver`) that fetches environment-specific paths (`DWH_HOME`, `HOME`, `PMS_HOME`) from the Airflow Metadata Database (Airflow Variables). |
| `dags/utils/logging_helper.py` | Execution Logging Helper | A Python function (`log_execution_details_callable`) that extracts the active DAG ID and Task ID from the Airflow context and writes a standardized log entry. |

---

## 3. Key Design Decisions

### Modularization over Standalone DAGs
In UC4, `JOBI` objects are compiled inline prior to job execution and cannot run independently. To mirror this behavior natively in Airflow, these scripts were **not** migrated to standalone DAGs. Instead, they were refactored into a shared Python utility package (`dags/utils/`). This approach:
* Prevents DAG clutter in the Cloud Composer UI.
* Enables downstream DAGs to import these utilities natively using standard Python import statements.
* Promotes code reusability and simplifies maintenance.

### Airflow Variables for Environment Configuration
The original `DW.HOLE_PFAD_VTRG` script resolved paths dynamically from a central UC4 variable container (`DW.VARIABLEN`). In the target architecture, these have been mapped to **Airflow Variables** (`dw_variablen_dwh_home`, `dw_variablen_home`, and `dw_variablen_pms_home`). This decouples environment-specific configurations (e.g., Dev, Test, Prod paths) from the pipeline code.

### Preservation of Log Literals
To ensure operational continuity and compatibility with any legacy log-parsing tools, the German log literal from `DW.LESE_LOG_VTRG` has been preserved character-for-character:
* **UC4:** `Protokolleintrag: &ADMJOB innerhalb &ADMJP`
* **Airflow:** `f"Protokolleintrag: {task_id} innerhalb {parent_dag_id}"`

---

## 4. Manual Steps Before Go-Live

### 1. Schema & Dataset Creation
Ensure that the target Google Cloud Storage (GCS) buckets and BigQuery datasets referenced by downstream processes exist and are configured according to your landing zone specifications.

### 2. IAM & Permissions
The Cloud Composer Environment Service Account must have the following permissions:
* `roles/composer.worker` (to execute the utility tasks).
* `roles/logging.logWriter` (to forward standard output and Python logs to Google Cloud Logging/Stackdriver).

### 3. Airflow Variables Setup
Before executing any downstream DAGs that import `path_resolver.py`, the following variables must be created in the Airflow Metadata Database. This can be done via the Airflow UI (**Admin -> Variables**) or the gcloud CLI:

```bash
# Example CLI commands to set variables in Cloud Composer
gcloud composer environments run <ENVIRONMENT_NAME> \
    --location <LOCATION> \
    variables set -- dw_variablen_dwh_home "/opt/dwh"

gcloud composer environments run <ENVIRONMENT_NAME> \
    --location <LOCATION> \
    variables set -- dw_variablen_home "/home/airflow"

gcloud composer environments run <ENVIRONMENT_NAME> \
    --location <LOCATION> \
    variables set -- dw_variablen_pms_home "/opt/pms"
```

### 4. Scheduling
Because these are helper utilities, they do not have independent schedules. They inherit the scheduling and execution properties of the parent DAGs that import them.

---

## 5. Known Gaps & Unresolved References

### Redesign (B4) / Missing Parent Contexts
* **Unmigrated Downstream Consumers:** The primary downstream consumers of these includes (such as `DW.DWH_VERTRAG_TARIF_SYNC_START_JS` and `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS`) have not yet been migrated. 
* **Integration Action:** Once those parent workflows are migrated to Cloud Composer, their DAG definitions must be updated to import these utilities.
  * **Example Import for Path Resolution:**
    ```python
    from utils.path_resolver import DynamicPathResolver
    paths = DynamicPathResolver.get_paths()
    ```
  * **Example Import for Logging:**
    ```python
    from utils.logging_helper import log_execution_details_callable
    
    log_task = PythonOperator(
        task_id='log_execution_details',
        python_callable=log_execution_details_callable,
        provide_context=True,
        dag=dag
    )
    ```

---

## 6. Validation

### Unit Testing the Utilities
To validate the migrated files independently of a parent DAG, run the following test scripts within your local development environment or Cloud Composer worker terminal.

#### Test 1: Path Resolver Validation
Create a temporary Python script `test_path_resolver.py`:
```python
import unittest
from unittest.mock import patch
from utils.path_resolver import DynamicPathResolver

class TestPathResolver(unittest.TestCase):
    @patch('airflow.models.Variable.get')
    def test_get_paths_success(self, mock_variable_get):
        # Mock Airflow Variable returns
        mock_variable_get.side_effect = lambda key: {
            "dw_variablen_dwh_home": "/mock/dwh",
            "dw_variablen_home": "/mock/home",
            "dw_variablen_pms_home": "/mock/pms"
        }[key]
        
        paths = DynamicPathResolver.get_paths()
        self.assertEqual(paths["DWH_HOME"], "/mock/dwh")
        self.assertEqual(paths["HOME"], "/mock/home")
        self.assertEqual(paths["PMS_HOME"], "/mock/pms")

if __name__ == '__main__':
    unittest.main()
```

#### Test 2: Logging Helper Validation
Create a temporary Python script `test_logging_helper.py`:
```python
import unittest
from unittest.mock import MagicMock, patch
from utils.logging_helper import log_execution_details_callable

class TestLoggingHelper(unittest.TestCase):
    @patch('logging.info')
    def test_logging_output(self, mock_log_info):
        # Mock Airflow context dictionary
        mock_context = {
            'dag': MagicMock(dag_id='test_parent_dag'),
            'task_instance': MagicMock(task_id='test_child_task')
        }
        
        log_execution_details_callable(**mock_context)
        mock_log_info.assert_called_once_with("Protokolleintrag: test_child_task innerhalb test_parent_dag")

if __name__ == '__main__':
    unittest.main()
```

### Definition of "Passing"
* **Path Resolver:** The test successfully mocks the Airflow Variable retrieval and returns a dictionary containing the correct keys and values without raising an `AirflowException`.
* **Logging Helper:** The test successfully extracts the context parameters and outputs the exact German log string format to the logging framework.

---

## 7. Rollback Procedure

In the event of an issue or deployment failure:

1. **Remove Target Files:** Delete the migrated utility files from the Cloud Composer GCS bucket:
   ```bash
   gsutil rm gs://<composer-bucket>/dags/utils/path_resolver.py
   gsutil rm gs://<composer-bucket>/dags/utils/logging_helper.py
   ```
2. **Clean Up Variables (Optional):** If necessary, remove the environment variables created during the manual setup phase:
   ```bash
   gcloud composer environments run <ENVIRONMENT_NAME> \
       --location <LOCATION> \
       variables delete -- dw_variablen_dwh_home
   ```
3. **Verify Downstream Stability:** Ensure that any downstream DAGs that were modified to import these utilities are reverted to their previous stable state to prevent import errors (`ModuleNotFoundError`).