# Migration Notes: Shared Files — DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes

## 1. Summary
The migration converts legacy UC4 Include (`JOBI`) scripts into a modular, reusable Python utility module designed for **Google Cloud Composer (Apache Airflow)**. 

The following legacy UC4 components have been consolidated and migrated:
*   **`DW.HOLE_PFAD_KNZB`**: Originally queried the global UC4 variable container `DW.VARIABLEN` to resolve system paths (`&DWH_HOME`, `&HOME`, and `&ISTNS_HOME`).
*   **`DW.LESE_LOG_KNZB`**: Originally captured parent execution context (`SYS_ACT_JPNAME()` and `SYS_ACT_JOBNAME()`) to write a standardized tracking entry (`Protokolleintrag`) to the run log.

These components are now unified into a single Python utility file, `dw_dwh_stamm_includes.py`, which downstream Airflow DAGs can import to resolve environment paths and perform standardized logging.

---

## 2. Generated Artifacts
The migration produces a single consolidated Python module:

| Target File Path | Role | Description |
| :--- | :--- | :--- |
| `dags/utils/dw_dwh_stamm_includes.py` | Shared Utility Module | Contains `hole_pfad_knzb()` for path resolution and `log_parent_context()` for standardized execution logging. |

---

## 3. Key Design Decisions
*   **Consolidation of JOBI Objects:** In UC4, `JOBI` objects are textually substituted into jobs at runtime. In Airflow, creating separate tasks or DAGs for these fragments would introduce unnecessary scheduling overhead. They are consolidated into a single Python utility module (`dw_dwh_stamm_includes.py`) to optimize project structure and leverage native Python imports.
*   **Airflow Variable Store Integration:** The legacy UC4 variable container `DW.VARIABLEN` is mapped directly to the **Airflow Variable Store** (backed by Cloud Composer's metadata database or GCP Secret Manager).
*   **Fail-Safe Logging:** The `log_parent_context` function is wrapped in a `try-except` block. This ensures that any non-critical logging failure does not interrupt or fail the core business workflow.
*   **Cloud-Native Path Mapping:** Legacy Unix filesystem paths are mapped to Google Cloud Storage (GCS) URI paths (e.g., `gs://YOUR_BUCKET_NAME/...`) to align with cloud-native architectures.

---

## 4. Manual Steps Before Go-Live

### Schema & Dataset Creation
*   Ensure the target Google Cloud Storage (GCS) buckets referenced in your environment paths exist and are accessible by the Cloud Composer service account.

### IAM & Permissions
*   The Cloud Composer environment's service account must have the **Storage Object Viewer** (or Creator, depending on downstream tasks) role on the GCS buckets resolved by `hole_pfad_knzb()`.

### Airflow Variables Configuration
You must configure the following Airflow Variables in your Cloud Composer environment before executing any consumer DAGs. This can be done via the Airflow UI (**Admin -> Variables**), the gcloud CLI, or GCP Secret Manager:

| Airflow Variable Key | Expected Value / Format | Description |
| :--- | :--- | :--- |
| `GCS_BUCKET` | `gs://your-environment-bucket` | The root GCS bucket for the environment. |
| `dw_variablen_dwh_home` | `gs://your-environment-bucket/dwh/home` | Equivalent to legacy `&DWH_HOME`. |
| `dw_variablen_home` | `gs://your-environment-bucket/home` | Equivalent to legacy `&HOME`. |
| `dw_variablen_istns_home` | `gs://your-environment-bucket/istns_home` | Equivalent to legacy `&ISTNS_HOME`. |

### Scheduling & Integration
*   Because these are include utilities, they do not have independent schedules.
*   Downstream DAGs must import this utility. Ensure the file is deployed to the `dags/utils/` directory in your Composer environment's GCS bucket.

---

## 5. Known Gaps & Unresolved References
*   **Downstream Consumer Wiring:** The downstream jobs `DW.DWH_STAMM_KNZB_ABGL_START_JS` and `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` are currently unmigrated. Once migrated, they must be explicitly modified to import and invoke these helper functions:
    ```python
    from utils.dw_dwh_stamm_includes import hole_pfad_knzb, log_parent_context
    ```
*   **Redesign (B4) Items:** Legacy hardcoded Unix paths in downstream scripts must be refactored to use the GCS paths returned by `hole_pfad_knzb()`.

---

## 6. Validation

### Unit Testing the Utility
To validate the utility independently of a running Airflow cluster, you can run a local Python test script using mock variables:

```python
import unittest
from unittest.mock import patch
from dags.utils.dw_dwh_stamm_includes import hole_pfad_knzb

class TestDwhStammIncludes(unittest.TestCase):
    
    @patch('airflow.models.Variable.get')
    def test_hole_pfad_knzb(self, mock_variable_get):
        # Mock the Airflow Variable store responses
        mock_variable_get.side_effect = lambda key, default_var=None: {
            "GCS_BUCKET": "gs://test-bucket",
            "dw_variablen_dwh_home": "gs://test-bucket/dwh/home",
            "dw_variablen_home": "gs://test-bucket/home",
            "dw_variablen_istns_home": "gs://test-bucket/istns_home"
        }.get(key, default_var)
        
        paths = hole_pfad_knzb()
        self.assertEqual(paths["DWH_HOME"], "gs://test-bucket/dwh/home")
        self.assertEqual(paths["HOME"], "gs://test-bucket/home")
        self.assertEqual(paths["ISTNS_HOME"], "gs://test-bucket/istns_home")

if __name__ == '__main__':
    unittest.main()
```

### What "Passing" Means
1.  **Path Resolution:** `hole_pfad_knzb()` successfully returns a dictionary containing the three path keys, resolving to either the configured Airflow Variables or the safe fallback defaults.
2.  **Logging Output:** When `log_parent_context()` is executed within an Airflow task context, the task execution logs must display the structured boundary log:
    ```text
    ============================================================
    Protokolleintrag: [TASK_ID] innerhalb [DAG_ID]
    ============================================================
    ```
3.  **Fail-Safe:** If the Airflow context is missing or corrupted during execution, `log_parent_context()` must catch the exception, log a warning, and allow the process to continue without raising an error.

---

## 7. Rollback Procedure
If issues are detected with the migrated includes in production:
1.  **Revert Consumer DAG Imports:** Revert any changes in downstream consumer DAGs that import `dags/utils/dw_dwh_stamm_includes.py`.
2.  **Restore Airflow Variables:** If path resolution errors occur, verify and restore the Airflow Variable values in the Airflow UI to their last known stable configurations.
3.  **File Removal:** If necessary, remove the `dw_dwh_stamm_includes.py` file from the `dags/utils/` folder in the Composer GCS bucket to prevent import conflicts.