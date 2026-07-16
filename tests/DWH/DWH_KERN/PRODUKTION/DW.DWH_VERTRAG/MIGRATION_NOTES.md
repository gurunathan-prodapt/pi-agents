# Migration Notes: `DW.DWH_VERTRAG_TARIF_SYNC_JP`

These migration notes detail the transition of the legacy UC4 job `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_JP.xml` and its associated components to Apache Airflow on Google Cloud Composer.

---

## 1. Summary

The legacy UC4 workflow `DW.DWH_VERTRAG_TARIF_SYNC_JP` has been migrated to a native Apache Airflow DAG. This workflow orchestrates a weekly synchronization loop for contract and tariff assignments between the `STAMMDATEN` and `DWH_KERN` database schemas. 

*   **Source Platform:** Automic UC4 (XML-based job plans, scripts, and include blocks).
*   **Target Platform:** Google Cloud Composer (Apache Airflow 2.x / Python 3).
*   **Migration Pattern:** **UC4_ONLY** (Direct translation of orchestration logic, environment setup, and state-locking mechanisms into Python and Airflow operators).

---

## 2. Generated Artifacts

To preserve the source repository's exact folder structure and comply with the **Folder Integrity Rule**, the legacy components have been mapped to the following target files:

| Target File Path | Role / Description |
| :--- | :--- |
| `dags/dwh/dwh_kern/produktion/dw_dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py` | **Primary Orchestrator DAG:** Contains the main workflow definition, task execution graph, and folded logic from the legacy start (`_START_JS`) and end (`_ENDE_JS`) scripts. |
| `dags/dwh/dwh_kern/produktion/dw_dwh_vertrag/includes/dw_hole_pfad_vtrg.py` | **Path Helper Module:** Replaces legacy JOBI `DW.HOLE_PFAD_VTRG`. Resolves system-specific and global environmental path hierarchies. |
| `dags/dwh/dwh_kern/produktion/dw_dwh_vertrag/includes/dw_lese_log_vtrg.py` | **Logging Utility Module:** Replaces legacy JOBI `DW.LESE_LOG_VTRG`. Standardizes execution logs and operational metadata tracking. |

---

## 3. Key Design Decisions

### Folder Integrity & Modular Imports
To prevent cross-directory pollution, the legacy include files (`includes/`) are isolated into their own Python modules matching the source directory structure. The main DAG imports these modules using absolute Python import paths:
```python
from dags.dwh.dwh_kern.produktion.dw_dwh_vertrag.includes.dw_hole_pfad_vtrg import get_vtrg_paths
from dags.dwh.dwh_kern.produktion.dw_dwh_vertrag.includes.dw_lese_log_vtrg import log_uc4_metadata
```

### State & Concurrency Lock Mapping
The legacy UC4 variable containers (`DW.VARIABLEN` and `DW.VARIABLEN_VTRG`) have been mapped directly to **Airflow Variables**. 
*   `dw_variablen_vtrg_sync_status` acts as a state lock (`FREI`, `LAEUFT`, `GESPERRT`).
*   `dw_variablen_vtrg_letzter_lauf` stores the execution date (`YYYYMMDD`).

### Branching & Failure Simulation
*   A `BranchPythonOperator` (`check_sync_status`) evaluates the lock state.
*   If the lock is `GESPERRT`, the workflow routes to `abort_execution` which raises an `AirflowFailException`, mimicking UC4's `STOP_JOB()` command.
*   If the lock is `FREI`, the workflow routes to `update_sync_variables`, sets the state to `LAEUFT`, executes the core sync, and releases the lock (`FREI`) upon successful completion.

### Verbatim Log Preservation
To ensure operational continuity and compatibility with legacy log parsers, all German log messages, print statements, and error messages are preserved character-for-character (e.g., `"Vertrags-/Tarifabgleich fuer {lauf_datum} ist gesperrt - Abbruch"`).

---

## 4. Manual Steps Before Go-Live

The following configuration steps must be performed in the target Airflow environment before enabling the DAG:

### 1. Airflow Variables Initialization
Create the following Airflow Variables via the Airflow UI (**Admin -> Variables**) or the CLI:

| Variable Key | Expected Value / Format | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `your-gcp-project-id` | Target Google Cloud Project ID. |
| `GCS_BUCKET` | `your-composer-bucket-name` | GCS bucket designated for environment storage. |
| `dw_variablen_vtrg_sync_status` | `"FREI"` | Initial lock state. Must be set to `"FREI"` to allow the first run. |
| `dw_variablen_vtrg_letzter_lauf` | `"19700101"` | Initial placeholder date for the last run. |
| `dw_variablen_paths` | `{"dwh_home": "/opt/dwh", "home": "/home/dwarf", "pms_home": "/opt/pms"}` | JSON-serialized dictionary of system paths. |

### 2. IAM & Permissions
Ensure that the Cloud Composer Service Account has:
*   `roles/composer.worker`
*   Read/Write access to the GCS bucket specified in `GCS_BUCKET`.

### 3. Scheduling
The DAG is configured to run weekly on Sundays at 03:00 AM (`0 3 * * 7`). Verify that this window does not conflict with database maintenance cycles in `STAMMDATEN` or `DWH_KERN`.

---

## 5. Known Gaps & Unresolved References

### Core Execution Placeholder
*   **Gap:** The legacy UC4 job plan contained a placeholder or external call for the actual table modification/synchronization logic.
*   **Current Implementation:** Represented in the DAG as an `EmptyOperator` named `core_sync_execution`.
*   **Redesign (B4) Action Item:** Before go-live, replace `core_sync_execution` with the actual data-transfer operator (e.g., `BigQueryOperator`, `PostgresOperator`, or a custom Python/Bash operator depending on where the target tables reside).

---

## 6. Validation

### Local Unit Testing
Verify that the DAG parses without syntax or import errors:
```bash
python3 dags/dwh/dwh_kern/produktion/dw_dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py
```

### Airflow CLI Validation
Verify that the DAG is recognized by Airflow:
```bash
airflow dags list | grep dw_dwh_vertrag_tarif_sync_jp
```

### Execution Path Testing
1.  **Test Path A (Blocked):**
    *   Manually set the Airflow Variable `dw_variablen_vtrg_sync_status` to `"GESPERRT"`.
    *   Trigger the DAG.
    *   **Passing Criteria:** The task `check_sync_status` branches to `abort_execution`, and the DAG run ends with a `FAILED` status. The logs must contain: `Vertrags-/Tarifabgleich fuer <DATE> ist gesperrt - Abbruch`.
2.  **Test Path B (Successful Run):**
    *   Manually set the Airflow Variable `dw_variablen_vtrg_sync_status` to `"FREI"`.
    *   Trigger the DAG.
    *   **Passing Criteria:** The task `check_sync_status` branches to `update_sync_variables`. The variable `dw_variablen_vtrg_sync_status` temporarily changes to `"LAEUFT"`. The DAG completes successfully, resetting the variable back to `"FREI"`.

---

## 7. Rollback Procedure

In the event of an execution failure, database corruption, or scheduling conflict post-deployment:

1.  **Pause the DAG:** Immediately pause the DAG in the Airflow UI or via the CLI:
    ```bash
    airflow dags pause dw_dwh_vertrag_tarif_sync_jp
    ```
2.  **Reset State Lock:** Force-reset the synchronization variable to prevent downstream blocking:
    ```bash
    airflow variables set dw_variablen_vtrg_sync_status "FREI"
    ```
3.  **Revert Code:** Roll back the deployment in the Git repository to the previous stable tag and redeploy the DAG files to the Composer `/dags` bucket.