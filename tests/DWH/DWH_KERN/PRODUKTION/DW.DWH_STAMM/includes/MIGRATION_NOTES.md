# Migration Notes

**Job:** Shared Files — `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes`  
**Target Platform:** Cloud Composer (Apache Airflow)  
**Migration Pattern:** `UC4_ONLY` (Shared Utility Modules)

---

## 1. Summary

This migration covers the transition of two UC4 Include Scripts (`JOBI` objects) from the legacy UC4 scheduler to Google Cloud Composer (Apache Airflow). 

Because these source files do not define standalone workflows, they have been refactored into **reusable Python utility modules** rather than independent DAGs. This approach ensures that downstream DAGs can import and execute these shared routines, preserving the original environment path resolution and structured audit logging behaviors.

*   **Source Path:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/`
*   **Target Platform:** Cloud Composer (Apache Airflow)
*   **Migration Strategy:** Reusable Airflow Utility Modules (`plugins/utils/`)

---

## 2. Generated Artifacts

The following files have been generated to replicate the legacy UC4 include logic:

| Source File Path | Target File Path | Role / Description |
| :--- | :--- | :--- |
| `DW.HOLE_PFAD_KNZB.xml` | `plugins/utils/dw_hole_pfad_knzb.py` | Utility module to resolve and validate environment path variables (`DWH_HOME`, `HOME`, `ISTNS_HOME`) from Airflow Variables or environment fallbacks. |
| `DW.LESE_LOG_KNZB.xml` | `plugins/utils/dw_lese_log_knzb.py` | Utility logging module that extracts execution context and prints structured, legacy-compliant German audit logs. |

---

## 3. Key Design Decisions

### Reusable Module Pattern (`UC4_ONLY`)
Because `JOBI` objects are non-executable code snippets in UC4, converting them into standalone Airflow DAGs would introduce unnecessary scheduling overhead and empty task runs. Instead, they are migrated as Python modules placed in the Airflow `plugins/utils/` directory. This allows any downstream DAG to import them directly.

### Dynamic Path Resolution with Fallbacks
In `dw_hole_pfad_knzb.py`, the path resolution logic first attempts to retrieve values from Airflow Variables (`dw_variablen_dwh_home`, etc.). If these are not configured, it dynamically constructs Google Cloud Storage (GCS) URI fallbacks using the `GCS_BUCKET` environment variable. This prevents hardcoding and supports multi-environment deployments (Dev, QA, Prod).

### Non-Blocking Logging Execution
The logging utility `dw_lese_log_knzb.py` is designed to fail gracefully. If the execution context cannot be parsed, the error is caught and logged as a warning, ensuring that a failure in the metadata logging step never crashes the parent data pipeline.

### Preservation of Legacy Audit Syntax
To maintain compatibility with legacy log parsers and automated auditing tools, the German log format has been preserved character-for-character:
`"Protokolleintrag: {adm_job} innerhalb {adm_jp}"`

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variables Configuration
The following Airflow Variables must be configured in the Cloud Composer environment (via the Airflow UI, CLI, or a connected Secrets Manager):

*   **`dw_variablen_dwh_home`**: The GCS URI or path for the core DWH root (e.g., `gs://your-environment-bucket/dwh/`).
*   **`dw_variablen_home`**: The GCS URI or path for the home namespace (e.g., `gs://your-environment-bucket/home/`).
*   **`dw_variablen_istns_home`**: The GCS URI or path for the instance home namespace (e.g., `gs://your-environment-bucket/istns/`).

### 2. Environment Variables
Ensure that the `GCS_BUCKET` environment variable is set in your Cloud Composer environment configuration to enable dynamic fallback path generation.

### 3. IAM & Permissions
Ensure that the Cloud Composer Worker Service Account has appropriate IAM permissions (`roles/storage.objectViewer` or `roles/storage.objectAdmin`) on the GCS buckets resolved by `dw_hole_pfad_knzb.py`.

### 4. Deployment Location
Copy the generated files to the Airflow environment's plugins directory:
*   `dw_hole_pfad_knzb.py` -> `/home/airflow/gcs/plugins/utils/dw_hole_pfad_knzb.py`
*   `dw_lese_log_knzb.py` -> `/home/airflow/gcs/plugins/utils/dw_lese_log_knzb.py`

---

## 5. Known Gaps & Unresolved References

*   **Parent Workflows Missing (Redesign B4):** The parent workflows (`JOBP`) and Unix jobs (`JOBS_UNIX`) that call these includes—specifically `DW.DWH_STAMM_KNZB_ABGL_START_JS` and `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS`—were not part of this migration scope. 
*   **Action Required:** When those downstream workflows are migrated, developers must manually add imports to these utility modules at the start of their respective task definitions.

---

## 6. Validation

### Unit Testing the Utilities
To validate the modules independently of a parent DAG, run the following Python tests within the Cloud Composer environment or a local development container:

```python
# test_includes.py
import os
from unittest.mock import MagicMock
from plugins.utils.dw_hole_pfad_knzb import get_path_variables
from plugins.utils.dw_lese_log_knzb import log_uc4_metadata

# Setup mock environment
os.environ["GCS_BUCKET"] = "test-bucket"

def test_path_resolution():
    paths = get_path_variables()
    assert paths["DWH_HOME"] == "gs://test-bucket/dwh/"
    assert paths["HOME"] == "gs://test-bucket/home/"
    assert paths["ISTNS_HOME"] == "gs://test-bucket/istns/"
    print("Path resolution validation: PASSED")

def test_logging_output():
    mock_context = {
        "dag": MagicMock(dag_id="TEST_DAG"),
        "task_instance": MagicMock(task_id="TEST_TASK")
    }
    # Should execute without raising exceptions
    log_uc4_metadata(mock_context)
    print("Logging validation: PASSED")

if __name__ == "__main__":
    test_path_resolution()
    test_logging_output()
```

### What "Passing" Means
1.  **`get_path_variables()`** successfully returns a dictionary containing the three target paths without throwing an `AirflowException`.
2.  **`log_uc4_metadata()`** successfully writes the formatted log line containing `TEST_TASK` and `TEST_DAG` to the standard output/log stream without raising any exceptions.

---

## 7. Rollback Procedure

In the event of an issue during deployment or runtime:

1.  **Remove Utility Files:** Delete the migrated files from the Cloud Composer GCS plugins folder:
    ```bash
    gcloud storage rm gs://<your-composer-bucket>/plugins/utils/dw_hole_pfad_knzb.py
    gcloud storage rm gs://<your-composer-bucket>/plugins/utils/dw_lese_log_knzb.py
    ```
2.  **Revert Downstream Imports:** If downstream DAGs have already been modified to import these modules, revert those DAG files to their previous state to prevent import errors.
3.  **Clear Variables (Optional):** If desired, remove the custom Airflow Variables (`dw_variablen_dwh_home`, `dw_variablen_home`, `dw_variablen_istns_home`) via the Airflow UI or CLI.