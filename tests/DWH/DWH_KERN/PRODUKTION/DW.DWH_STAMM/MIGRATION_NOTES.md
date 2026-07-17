# MIGRATION_NOTES.md

## 1. Summary
This document details the migration of the legacy UC4/Automic Job Plan `DW.DWH_STAMM_KNZB_ABGL_JP` (and its associated jobs and includes) to Google Cloud Platform (GCP). 

* **Legacy Source Component**: `DW.DWH_STAMM_KNZB_ABGL_JP` (Daily reconciliation workflow for customer numbers and basic access master data).
* **Source Type**: UC4/Automic Job Plan (`JOBP`), Jobs (`JOBS`), and Includes (`JOBI`).
* **Target Platform**: Google Cloud Platform (GCP)
* **Target Orchestrator**: Cloud Composer (Apache Airflow)
* **Migration Strategy**: 1:1 Airflow DAG migration of the pure orchestration logic.
* **Migration Pattern**: `UC4_ONLY` — Pure orchestration, concurrency locking, and variable maintenance. No direct data-plane transformations exist in this specific job chain.

---

## 2. Generated Artifacts
The following target files have been generated to mirror the legacy structure while complying with the **Folder Integrity Rule**:

1. **`dags/DW/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW_DWH_STAMM_KNZB_ABGL_JP.py`**
   * **Role**: Primary Airflow DAG orchestration file. It defines the workflow structure, tasks, and execution sequence. It folds the logic of the start job (`DW.DWH_STAMM_KNZB_ABGL_START_JS`) and end job (`DW.DWH_STAMM_KNZB_ABGL_ENDE_JS`) into Python operators.
2. **`dags/DW/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW_HOLE_PFAD_KNZB.py`**
   * **Role**: Shared utility module translating the legacy include `DW.HOLE_PFAD_KNZB`. It retrieves environment-wide path variables.
3. **`dags/DW/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW_LESE_LOG_KNZB.py`**
   * **Role**: Shared utility module translating the legacy include `DW.LESE_LOG_KNZB`. It handles standardized logging output.

---

## 3. Key Design Decisions
* **Pure Python Orchestration**: Because this workflow serves as a control-plane lock/unlock mechanism (checking and setting execution states), it is implemented entirely using Airflow `PythonOperator` tasks. This avoids the overhead of launching external containers or VMs.
* **State Management via Airflow Variables**: The legacy UC4 variable container `DW.VARIABLEN_KNZB` is mapped directly to an Airflow Variable (`DW_VARIABLEN_KNZB`) stored as a JSON object. This preserves the stateful behavior of `ABGLEICH_STATUS` and `LETZTER_LAUF` across DAG runs.
* **Folder Integrity & Modular Imports**: To prevent multi-directory mixing, legacy includes (`JOBI`) are compiled into separate Python modules within a dedicated `includes/` subfolder matching their legacy paths. The main DAG dynamically appends this subfolder to `sys.path` to resolve imports cleanly.
* **Preservation of German Log Literals**: All original German log outputs and error messages (e.g., `"KNZB-Abgleich fuer {lauf_datum} ist gesperrt - Abbruch der Verarbeitung"`) have been preserved verbatim to maintain operational continuity and support legacy log-parsing patterns.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variable Initialization
Before executing the DAG for the first time, you must define the following variables in the Airflow Metadata Database (via the Airflow UI under **Admin -> Variables** or via the CLI):

* **`DW_VARIABLEN`** (JSON):
  ```json
  {
    "DWH_HOME": "/opt/dwh",
    "HOME": "/home/airflow",
    "ISTNS_HOME": "/opt/istns"
  }
  ```
* **`DW_VARIABLEN_KNZB`** (JSON):
  ```json
  {
    "ABGLEICH_STATUS": "FREI",
    "LETZTER_LAUF": ""
  }
  ```

### 2. IAM & Permissions
* Ensure the Cloud Composer Service Account has the **Composer Worker** role (`roles/composer.worker`) and permissions to read/write Airflow Variables.

### 3. Scheduling & Concurrency
* The DAG is configured to run daily at `04:00 UTC` (`0 4 * * *`). Verify that this window does not conflict with upstream master data extraction schedules.
* **Concurrency Lock**: If multiple DAGs or external processes modify `DW_VARIABLEN_KNZB` concurrently, assign this DAG to an Airflow Pool with a slot size of `1` to prevent race conditions.

---

## 5. Known Gaps & Unresolved References
* **Shared State Race Conditions**: Airflow Variables are not fully transactional. If another migrated workflow attempts to read or write to `DW_VARIABLEN_KNZB` at the exact same millisecond, a race condition could occur. 
  * *Follow-up (Redesign B4)*: For a more robust cloud-native architecture, consider migrating this state-locking mechanism to a Firestore document or a Cloud SQL table using row-level locking.
* **Hardcoded Fallback Paths**: The fallback paths in `DW_HOLE_PFAD_KNZB.py` (e.g., `/opt/dwh`) are legacy POSIX paths. If the target environment does not use these directories, ensure the `DW_VARIABLEN` Airflow Variable is correctly populated with cloud-appropriate paths (e.g., GCS bucket URIs `gs://...` if applicable).

---

## 6. Validation

### How to Run the Tests
1. **Syntax & DAG Import Test**:
   Run the following command in your local development environment or Composer terminal to ensure there are no import errors:
   ```bash
   python3 dags/DW/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW_DWH_STAMM_KNZB_ABGL_JP.py
   ```
2. **Unit Testing Task Execution**:
   Test individual tasks using the Airflow CLI:
   ```bash
   airflow tasks test DW_DWH_STAMM_KNZB_ABGL_JP DW_DWH_STAMM_KNZB_ABGL_START_JS 2024-11-04
   ```

### What "Passing" Means
* **Successful Run (Locking)**:
  * When `DW_VARIABLEN_KNZB["ABGLEICH_STATUS"]` is `"FREI"`, running `DW_DWH_STAMM_KNZB_ABGL_START_JS` changes the status to `"LAEUFT"`, updates `LETZTER_LAUF` to the current date, and completes successfully.
  * Running `DW_DWH_STAMM_KNZB_ABGL_ENDE_JS` resets the status back to `"FREI"` and logs:
    `"KNZB-Stammdatenabgleich fuer Lauf <YYYYMMDD> erfolgreich beendet"`.
* **Aborted Run (Sperre)**:
  * If `DW_VARIABLEN_KNZB["ABGLEICH_STATUS"]` is set to `"GESPERRT"`, running the start task must immediately raise an `AirflowFailException`, fail the task, and log:
    `"KNZB-Abgleich fuer <YYYYMMDD> ist gesperrt - Abbruch der Verarbeitung"`.

---

## 7. Rollback Procedure
In the event of a deployment failure or unexpected behavior in production:

1. **Pause the DAG**: Immediately pause the DAG in the Airflow UI to prevent subsequent scheduled executions.
2. **Reset the State Variable**: Manually reset the state variable in the Airflow UI (**Admin -> Variables -> `DW_VARIABLEN_KNZB`**) to:
   ```json
   {
     "ABGLEICH_STATUS": "FREI",
     "LETZTER_LAUF": "<DATE_OF_LAST_SUCCESSFUL_LEGACY_RUN>"
   }
   ```
3. **Revert Code**: Revert the Git repository to the previous stable commit and redeploy the DAG folder to the Composer GCS bucket.
4. **Legacy Fallback**: If necessary, resume the legacy UC4 Job Plan `DW.DWH_STAMM_KNZB_ABGL_JP` on the legacy engine to ensure business continuity.