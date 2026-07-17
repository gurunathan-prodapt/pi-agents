# MIGRATION_NOTES.md

## 1. Summary
This migration covers the transition of the UC4 Job Includes (`JOBI`) located under the assembly path `Shared Files — DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes` to Google Cloud Composer (Apache Airflow).

*   **Source Components:** 
    *   `DW.HOLE_PFAD_VTRG` (UC4 Job Include): Dynamically resolved environment directory paths from the centralized UC4 variable container `DW.VARIABLEN`.
    *   `DW.LESE_LOG_VTRG` (UC4 Job Include): Logged execution tracking metadata (parent job and job plan names) to the UC4 execution log.
*   **Target Platform:** Google Cloud Composer (Apache Airflow) running on Google Cloud Platform (GCP).
*   **Migration Pattern:** **UC4_ONLY**. Because these source files are reusable include fragments rather than standalone executable workflows, they have been refactored into a single, reusable Python utility module (`uc4_helpers.py`) that can be imported and executed dynamically by target Airflow DAGs.

---

## 2. Generated Artifacts
The migration process has generated a single, consolidated Python helper module to replace both UC4 Job Includes, maintaining folder structure integrity:

| Target File Path | Component Role | Description |
| :--- | :--- | :--- |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/uc4_helpers.py` | Shared Python Utility Module | Implements `hole_pfad_vtrg()` to resolve environment paths and `lese_log_vtrg(context)` to output standardized execution logs. |

---

## 3. Key Design Decisions

### Refactoring Includes to Python Modules
In UC4, Job Includes (`JOBI`) are dynamically substituted into parent scripts at runtime. In Airflow, the most elegant and maintainable equivalent is a shared Python module. This avoids code duplication and allows any DAG in the `DW.DWH_VERTRAG` namespace to import and run these helpers natively.

### Variable Consolidation
To minimize database round-trips to the Airflow Metadata DB, `hole_pfad_vtrg()` is designed to look for a single, unified JSON Airflow Variable named `dw_variablen`. If this JSON block is missing, it gracefully falls back to individual key lookups (`dwh_home`, `home`, `pms_home`).

### Strict Output Compliance
To ensure compatibility with legacy log parsers and downstream monitoring tools, the exact German output structure from `DW.LESE_LOG_VTRG` has been preserved character-for-character:
`Protokolleintrag: {task_name} innerhalb {dag_name}`

### Trade-offs
*   **No Standalone DAGs:** Because these are utility scripts, they cannot be scheduled or run independently in the Airflow UI. They must be imported and invoked within an active Airflow task context.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variables Creation
You must register the required environment paths in the target Cloud Composer environment. This can be done via the Airflow UI (**Admin -> Variables**) or the `gcloud` CLI.

#### Option A: Unified JSON Variable (Recommended)
*   **Key:** `dw_variablen`
*   **Value (JSON):**
```json
{
  "DWH_HOME": "/path/to/dwh_home",
  "HOME": "/path/to/home",
  "PMS_HOME": "/path/to/pms_home"
}
```

#### Option B: Individual Variables (Fallback)
If not using the unified JSON block, create three separate variables:
*   `dwh_home` (e.g., `/path/to/dwh_home`)
*   `home` (e.g., `/path/to/home`)
*   `pms_home` (e.g., `/path/to/pms_home`)

### 2. IAM & Permissions
Ensure that the Cloud Composer Worker Service Account has read access to the Airflow Metadata database (default behavior) and any Secret Manager secrets if variables are backed by Google Cloud Secret Manager.

### 3. Scheduling & Connections
No scheduling or connection strings are required for these utility files. Scheduling is inherited entirely from the consuming parent DAGs.

---

## 5. Known Gaps & Unresolved References

### Downstream Consumers (B4 Redesign Items)
The following downstream execution consumers are identified but **not yet migrated**:
*   `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS`
*   `DW.DWH_VERTRAG_TARIF_SYNC_START_JS`

### Wiring Action Required
When these parent DAGs are migrated, they must be configured to import the helper module and invoke the functions within their task execution blocks:

```python
from DWH.DWH_KERN.PRODUKTION.DW.DWH_VERTRAG.includes.uc4_helpers import hole_pfad_vtrg, lese_log_vtrg

# Inside an Airflow PythonOperator or TaskFlow function:
def my_task_execution(**context):
    # 1. Log execution context (replaces DW.LESE_LOG_VTRG)
    lese_log_vtrg(context)
    
    # 2. Retrieve paths (replaces DW.HOLE_PFAD_VTRG)
    paths = hole_pfad_vtrg()
    dwh_home = paths["DWH_HOME"]
    # ... proceed with task logic
```

---

## 6. Validation

### Unit Testing the Utility Module
To validate the migration without running a full DAG, execute the following Python test script within the Composer environment or a local development container:

```python
import unittest
from unittest.mock import MagicMock, patch
from dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_VERTRAG.includes.uc4_helpers import hole_pfad_vtrg, lese_log_vtrg

class TestUC4Helpers(unittest.TestCase):

    @patch('airflow.models.Variable.get')
    def test_hole_pfad_vtrg_fallback(self, mock_variable_get):
        # Mock individual variable lookups
        def side_effect(key, default_var=None):
            mapping = {
                "dwh_home": "/opt/dwh",
                "home": "/home/airflow",
                "pms_home": "/opt/pms"
            }
            return mapping.get(key, default_var)
        
        mock_variable_get.side_effect = side_effect
        
        paths = hole_pfad_vtrg()
        self.assertEqual(paths["DWH_HOME"], "/opt/dwh")
        self.assertEqual(paths["HOME"], "/home/airflow")
        self.assertEqual(paths["PMS_HOME"], "/opt/pms")

    def test_lese_log_vtrg(self):
        # Mock Airflow task context
        mock_context = {
            "dag": MagicMock(dag_id="TEST_DAG_ID"),
            "task_instance": MagicMock(task_id="TEST_TASK_ID")
        }
        
        with patch('builtins.print') as mock_print:
            lese_log_vtrg(mock_context)
            # Verify exact German string output match
            mock_print.assert_called_once_with("Protokolleintrag: TEST_TASK_ID innerhalb TEST_DAG_ID")

if __name__ == '__main__':
    unittest.main()
```

### What "Passing" Means
1.  **Path Resolution:** `hole_pfad_vtrg()` successfully returns a dictionary containing the configured paths without throwing exceptions.
2.  **Log Output:** `lese_log_vtrg()` prints the exact string format `Protokolleintrag: <task_id> innerhalb <dag_id>` to stdout, ensuring compatibility with legacy log parsers.

---

## 7. Rollback Procedure

In the event of an issue or deployment failure:

1.  **Delete Helper File:** Remove the migrated helper file from the Cloud Storage DAGs bucket:
    ```bash
    gcloud storage rm gs://<your-composer-bucket>/dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/uc4_helpers.py
    ```
2.  **Remove Airflow Variables:** If necessary, delete the created Airflow variables via the Airflow CLI or UI:
    ```bash
    airflow variables delete dw_variablen
    ```
3.  **Revert Downstream Imports:** If downstream DAGs were already modified to import this helper, revert those DAG files to their previous state to prevent import errors.