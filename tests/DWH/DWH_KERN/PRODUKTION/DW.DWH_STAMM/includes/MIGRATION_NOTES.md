# Migration Notes: Shared Files — DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes

This document details the migration of UC4 Include (`JOBI`) objects to Python utility modules within Google Cloud Composer (Apache Airflow).

---

## 1. Summary

The legacy UC4 Include objects under `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes` have been migrated to modular, reusable Python helper scripts. 

* **Source Platform:** UC4 / Automic Workload Automation
* **Target Platform:** Google Cloud Composer (Apache Airflow) / Google Cloud Platform (GCP)
* **Migrated Components:**
  * `DW.HOLE_PFAD_KNZB` (UC4 Include for path resolution) $\rightarrow$ `dw_hole_pfad_knzb.py`
  * `DW.LESE_LOG_KNZB` (UC4 Include for context logging) $\rightarrow$ `dw_lese_log_knzb.py`

---

## 2. Generated Artifacts

The migration process generated the following files, located in the Airflow DAGs directory structure:

| Target File Path | Role / Description |
| :--- | :--- |
| `dags/utils/dw_hole_pfad_knzb.py` | **Path Resolution Utility:** Resolves global environment paths (`DWH_HOME`, `HOME`, `ISTNS_HOME`) by querying the Airflow Variable store. |
| `dags/utils/dw_lese_log_knzb.py` | **Context Logging Helper:** Extracts active DAG and Task metadata from the Airflow execution context and writes a standardized log entry. |

---

## 3. Key Design Decisions

### Modular Python Utilities vs. Inline Code Injection
In UC4, Include (`JOBI`) objects dynamically inject text/script blocks into parent jobs at runtime. In Apache Airflow, the standard equivalent is to package reusable logic into Python modules under the `dags/utils/` directory. This approach:
* Promotes code reusability and clean separation of concerns.
* Simplifies unit testing of utility logic independently of DAG execution.

### Strict vs. Fallback Path Resolution
For `dw_hole_pfad_knzb.py`, a strict resolution mode is implemented by default. If any required Airflow Variable is missing, the utility raises a `KeyError`. This prevents downstream tasks from executing with incomplete or invalid path configurations (e.g., writing to an undefined directory). An optional `use_fallback` flag is provided for local development or testing environments.

### Non-Blocking Logging Context
For `dw_lese_log_knzb.py`, the logging logic is wrapped in a robust `try-except` block. If metadata extraction fails, it logs a warning but does not raise an exception. This ensures that auxiliary logging infrastructure issues never block or fail critical core data pipelines.

---

## 4. Manual Steps Before Go-Live

The following configuration steps must be completed in the target environment before executing any parent DAGs that import these utilities.

### 1. Airflow Variables Provisioning
The path resolution utility expects specific keys in the Cloud Composer / Airflow Variable store. You must configure these variables via the Airflow UI (**Admin $\rightarrow$ Variables**) or the gcloud CLI:

```bash
# Example using gcloud composer CLI
gcloud composer environments run <ENVIRONMENT_NAME> \
    --location <LOCATION> \
    variables set dw_variablen_dwh_home "/gcs/<bucket_name>/dwh_home"

gcloud composer environments run <ENVIRONMENT_NAME> \
    --location <LOCATION> \
    variables set dw_variablen_home "/gcs/<bucket_name>/home/dwh_user"

gcloud composer environments run <ENVIRONMENT_NAME> \
    --location <LOCATION> \
    variables set dw_variablen_istns_home "/gcs/<bucket_name>/istns_home"
```

### 2. IAM & Permissions
Ensure that the Cloud Composer Service Account has read/write permissions to the Google Cloud Storage (GCS) buckets mapped to the variables above (typically `roles/storage.objectAdmin` or `roles/storage.objectViewer` depending on the directory's purpose).

### 3. Scheduling & Connections
Because these files are helper utilities, they do not require independent scheduling or connection strings. They inherit the execution context and connections of their parent DAGs.

---

## 5. Known Gaps & Unresolved References

### Unmigrated Downstream Consumers
The primary downstream parent jobs that consume these includes have not yet been migrated:
* `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS`
* `DW.DWH_STAMM_KNZB_ABGL_START_JS`

**Action Required:** Once these parent workflows are migrated to Airflow DAGs, they must be updated to import and call these utility modules.

### Redesign (B4) Items
* **Hardcoded Paths:** The legacy system relied on local filesystem paths (e.g., `/opt/dwh_home`). During parent DAG migration, verify if these paths should be updated to point directly to GCS buckets (`gs://...`) or local mount points inside the Composer worker pods (`/home/airflow/gcs/...`).

---

## 6. Validation

To validate the migrated utilities, run the following verification steps.

### Unit Testing
Create a test script (e.g., `tests/test_knzb_utils.py`) to verify the behavior of both modules:

```python
import unittest
from unittest.mock import patch
from dags.utils.dw_hole_pfad_knzb import get_knzb_paths

class TestKnzbUtils(unittest.TestCase):

    @patch('airflow.models.Variable.get')
    def test_get_knzb_paths_success(self, mock_get):
        # Mock Airflow Variable store
        mock_get.side_effect = lambda key: {
            "dw_variablen_dwh_home": "/opt/dwh_home",
            "dw_variablen_home": "/home/dwh_user",
            "dw_variablen_istns_home": "/opt/istns_home"
        }[key]

        paths = get_knzb_paths()
        self.assertEqual(paths["DWH_HOME"], "/opt/dwh_home")

    @patch('airflow.models.Variable.get')
    def test_get_knzb_paths_missing_key_raises_error(self, mock_get):
        mock_get.side_effect = KeyError("dw_variablen_home")
        with self.assertRaises(KeyError):
            get_knzb_paths(use_fallback=False)

if __name__ == '__main__':
    unittest.main()
```

### What "Passing" Means
1. **Path Resolution:** `get_knzb_paths()` successfully returns a dictionary containing all three paths when variables are present, and raises a clear `KeyError` when they are missing.
2. **Context Logging:** `log_uc4_context_helper()` executes without throwing exceptions, even when run outside of an active Airflow task context (e.g., during local testing), and outputs the exact German log template:
   `Protokolleintrag: <task_id> innerhalb <dag_id>`

---

## 7. Rollback Procedure

If issues are detected with these utilities in production, execute the following rollback steps:

1. **Revert DAG Imports:** If parent DAGs were modified to import these utilities, revert those DAG files to their previous stable versions in the Git repository.
2. **Redeploy to Composer:** Push the reverted DAGs to the Composer `dags/` folder.
3. **Variable Retention:** Do not delete the Airflow Variables (`dw_variablen_*`) unless they conflict with other systems, as they may be needed for future migration attempts.