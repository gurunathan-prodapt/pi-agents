# Migration Notes: Shared Files — DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes

## 1. Summary
This migration covers the transition of two critical UC4 Job Include (`JOBI`) components from the legacy Automic/UC4 scheduler to Google Cloud Composer (Apache Airflow):
* **`DW.HOLE_PFAD_VTRG`**: Central path-resolution utility.
* **`DW.LESE_LOG_VTRG`**: Standardized diagnostic and execution protocol logger.

These components have been migrated from their legacy physical paths to a mirrored, modular structure in the target environment:
* **Target Platform**: Google Cloud Composer (Apache Airflow) running on Google Cloud Platform (GCP).
* **Target Location**: `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/`

---

## 2. Generated Artifacts
The migration process generated two modular Python utility files, preserving the original directory layout:

| Target File Path | Role / Description |
| :--- | :--- |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/dw_hole_pfad_vtrg.py` | **Path Resolution Module**: Fetches environment-specific paths (`DWH_HOME`, `HOME`, `PMS_HOME`) from Airflow Variables or GCP Secret Manager and pushes them to XCom for downstream consumption. |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/dw_lese_log_vtrg.py` | **Diagnostic Logging Module**: Extracts runtime metadata (`dag_id`, `task_id`) from the Airflow context and writes a standardized protocol log. |

---

## 3. Key Design Decisions

### Modular Python Functions vs. Shell Scripts
In UC4, `JOBI` objects are raw text blocks injected directly into parent shell scripts before execution. In Airflow, executing these as shell scripts is an anti-pattern. Instead, they have been refactored into **reusable Python modules** that can be imported directly into parent DAGs or executed via a `PythonOperator`.

### State Sharing via Airflow XCom
To replicate the dynamic variable export of `DW.HOLE_PFAD_VTRG`, the migrated Python function pushes the resolved paths to Airflow's **XCom** engine. Downstream operators (such as `DataprocSubmitJobOperator` or `BashOperator`) can dynamically pull these paths using Jinja templating:
```python
"{{ task_instance.xcom_pull(task_ids='resolve_path_variables', key='DWH_HOME') }}"
```

### Preservation of Original Log Literals
To ensure compatibility with legacy log parsers and automated monitoring tools, the exact German log format from `DW.LESE_LOG_VTRG` has been preserved:
```python
logging.info(f"Protokolleintrag: {task_id} innerhalb {dag_id}")
```

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variables Configuration
The path resolution utility relies on Airflow Variables to mimic the legacy `DW.VARIABLEN` container. You must configure these variables in your Airflow environment (via the Airflow UI, CLI, or Terraform):

| Variable Key | Expected Value Example | Description |
| :--- | :--- | :--- |
| `dw_variablen_dwh_home` | `gs://your-gcs-bucket/dwh/` | Cloud Storage path replacing the legacy local `DWH_HOME` |
| `dw_variablen_home` | `gs://your-gcs-bucket/home/` | Cloud Storage path replacing the legacy local `HOME` |
| `dw_variablen_pms_home` | `gs://your-gcs-bucket/pms/` | Cloud Storage path replacing the legacy local `PMS_HOME` |

### 2. Environment Variables
Ensure that the `GCS_BUCKET` environment variable is set in your Cloud Composer environment. This is used as a fallback if the specific Airflow variables are missing.

### 3. IAM & Permissions
Ensure that the Cloud Composer Service Account has the following permissions:
* `roles/storage.objectViewer` (or `roles/storage.objectAdmin`) on the GCS buckets configured in the variables above.
* `roles/secretmanager.secretAccessor` if you choose to back Airflow Variables with GCP Secret Manager.

---

## 5. Known Gaps & Unresolved References

### 1. Downstream Consumers Not Yet Migrated
The downstream consumer jobs that depend on these includes are currently **not yet migrated**:
* `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS`
* `DW.DWH_VERTRAG_TARIF_SYNC_START_JS`

**Action Required**: Once these parent workflows are migrated, their DAG definitions must import and call these helper modules at task initialization.

### 2. Transition from POSIX to GCS Paths
The original UC4 variables pointed to local UNIX paths (e.g., `/opt/dwh/...`). All downstream PySpark, Spark-SQL, or Bash tasks must be validated to ensure they can process Cloud Storage URI schemes (`gs://...`) instead of local file paths.

---

## 6. Validation

### Unit Testing the Modules
You can validate the execution of these helper modules by running a local Python test script within your Airflow environment:

```python
# test_includes.py
import os
from unittest.mock import MagicMock
from dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_VERTRAG.includes.dw_hole_pfad_vtrg import resolve_dwh_paths
from dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_VERTRAG.includes.dw_lese_log_vtrg import execute_protocol_log

# Mock Airflow Context
mock_context = {
    'dag': MagicMock(dag_id='test_dag_id'),
    'ti': MagicMock(task_id='test_task_id')
}

# 1. Test Path Resolution
os.environ["GCS_BUCKET"] = "my-test-bucket"
paths = resolve_dwh_paths(**mock_context)
assert "DWH_HOME" in paths
print("Path Resolution Test: PASSED")

# 2. Test Logging Output
execute_protocol_log(**mock_context)
print("Logging Test: PASSED (Verify 'Protokolleintrag: test_task_id innerhalb test_dag_id' in stdout)")
```

### What "Passing" Means
1. **`resolve_dwh_paths`**: Must return a dictionary containing `DWH_HOME`, `HOME`, and `PMS_HOME` pointing to valid `gs://` paths, and successfully push these values to XCom without throwing an `AirflowException`.
2. **`execute_protocol_log`**: Must write the exact string `Protokolleintrag: <task_id> innerhalb <dag_id>` to the Airflow task execution logs.

---

## 7. Rollback Procedure

In the event of an issue during deployment or go-live, follow these rollback steps:

1. **Revert Variable Changes**: If paths were updated in the Airflow Variable store, revert the values of `dw_variablen_dwh_home`, `dw_variablen_home`, and `dw_variablen_pms_home` to their previous stable configurations.
2. **Remove Code Artifacts**: Delete the migrated files from the Cloud Composer DAGs bucket:
   ```bash
   gcloud storage rm gs://<your-composer-dags-bucket>/dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/dw_hole_pfad_vtrg.py
   gcloud storage rm gs://<your-composer-dags-bucket>/dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/dw_lese_log_vtrg.py
   ```
3. **Verify Parent DAGs**: Ensure that any parent DAGs that were modified to import these files are either disabled or reverted to their previous versions to prevent import errors.