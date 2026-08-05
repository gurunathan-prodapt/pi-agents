# Migration Notes: DW.EXTTEST_LEGACY_DWH

This document details the migration of the legacy UC4 job `DW.EXTTEST_LEGACY_DWH` and its associated KornShell script `r_legacy_ksh_dwh` to Apache Airflow (Google Cloud Composer) and Python 3.

---

## 1. Summary
The legacy UC4 job `DW.EXTTEST_LEGACY_DWH` (originally a `JOBS_UNIX` object) and its execution script `r_legacy_ksh_dwh` have been migrated to a modern, cloud-native architecture. 

* **Source Platform:** UC4 / Automic Scheduler & Unix (KornShell executing Oracle SQL*Plus)
* **Target Platform:** Google Cloud Composer (Apache Airflow) & Python 3
* **Migration Scope:** 
  * Converted the UC4 job definition into a single-task Airflow DAG wrapper.
  * Converted the legacy KornShell script `r_legacy_ksh_dwh` into a Python 3 script.
  * Retired obsolete dependencies, including the SQL*Plus database export script (`d_legacy_ksh_dwh.sql`) and UC4 include scripts (`DW.EXTTEST_HOLE_PFAD`, `DW.EXTTEST_LESE_LOG`), based on human-reviewed design decisions.

---

## 2. Generated Artifacts

The migration process generated the following files:

### 1. Airflow DAG Wrapper
* **File Path:** `vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/DW.EXTTEST_LEGACY_DWH.py`
* **Role:** Orchestrates the execution of the migrated Python script. It defines a standalone, unscheduled DAG (`dw_exttest_legacy_dwh`) containing a single `BashOperator` task that executes the converted Python script with the appropriate environment variables.

### 2. Migrated Python Script
* **File Path:** `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh.py`
* **Role:** Replaces the legacy KornShell script `r_legacy_ksh_dwh`. It handles command-line argument parsing (e.g., `-h`/`--help`), logs execution progress with timestamps, and manages execution flow. Because the underlying SQL*Plus script was retired, this script stubs out the database execution while preserving the original logging and error-handling semantics.

---

## 3. Key Design Decisions

### Python Conversion over Bash
The legacy KornShell script was converted to Python 3 rather than a standard `BashOperator` shell script. This aligns with modern cloud-native practices, simplifies error handling, and removes dependencies on legacy shell environments.

### SQL*Plus Bypass & Retirement
During the migration analysis, the underlying SQL*Plus script `d_legacy_ksh_dwh.sql` was human-confirmed as **not needed** (retired). Consequently, the database execution step has been bypassed/stubbed in the Python script. The script still retains its structural error-handling and logging capabilities to ensure compatibility with downstream monitoring.

### Unscheduled DAG Configuration
Because no parent Workflow (`JOBP`) or Schedule (`JSCH`) was provided in the source extraction, the DAG is configured with `schedule=None`. It is designed to be triggered manually, externally, or integrated into a parent controller DAG once the broader orchestration context is established.

### Elimination of UC4 Includes
The UC4 include scripts `:inc DW.EXTTEST_HOLE_PFAD` and `:inc DW.EXTTEST_LESE_LOG` were flagged as obsolete. Their functionality (path resolution and log parsing) is handled natively by Airflow's environment configuration and logging mechanisms.

---

## 4. Manual Steps Before Go-Live

To deploy and run this job in the target environment, complete the following manual steps:

### 1. Environment Variables & Airflow Variables
Ensure the following Airflow Variables are configured in the target Cloud Composer environment:
* `GCP_PROJECT`: The ID of your Google Cloud project.
* `GCS_BUCKET`: The GCS bucket used for Composer storage (e.g., `us-central1-composer-bucket`).

### 2. Script Deployment
Upload the migrated Python script to the GCS bucket directory mapped to the Airflow workers:
* **Source:** `vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh.py`
* **Target GCS Path:** `gs://<your-composer-bucket>/data/scripts/r_legacy_ksh_dwh.py`

### 3. DAG Deployment
Upload the Airflow DAG to the Composer DAGs folder:
* **Source:** `vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP/DW.EXTTEST_LEGACY_DWH.py`
* **Target GCS Path:** `gs://<your-composer-bucket>/dags/DW.EXTTEST_LEGACY_DWH.py`

### 4. IAM & Permissions
Ensure that the Cloud Composer Service Account has:
* Read access (`roles/storage.objectViewer`) to the GCS bucket containing the scripts.
* Execution permissions for Python scripts within the Composer worker environment.

---

## 5. Known Gaps & Unresolved References

### Downstream Dependency (`DW.EXTTEST_ABLAUFSTEUERUNG`)
* **Status:** Unresolved / Not Yet Migrated.
* **Description:** The legacy job `DW.EXTTEST_ABLAUFSTEUERUNG` consumes the output of this job. Because it is not yet migrated, end-to-end integration cannot be finalized.
* **Remediation:** Once `DW.EXTTEST_ABLAUFSTEUERUNG` is migrated to Airflow, wire the dependency using a `TriggerDagRunOperator` at the end of `dw_exttest_legacy_dwh`, or configure an `ExternalTaskSensor` in the downstream DAG.

---

## 6. Validation

To validate the migration, perform the following tests in a lower environment:

### Local/Command-Line Validation (Python Script)

#### 1. Test Help Argument:
```bash
python3 r_legacy_ksh_dwh.py -h
```
* **Expected Output:** Displays usage instructions and exits with code `0`.

#### 2. Test Successful Execution:
```bash
python3 r_legacy_ksh_dwh.py
```
* **Expected Output:** 
  ```
  YYYY-MM-DD HH:MM:SS Starting legacy_ksh_dwh export
  YYYY-MM-DD HH:MM:SS legacy_ksh_dwh export completed
  ```
  Exits with code `0`.

#### 3. Test Failure Path (Mocking):
```bash
export MOCK_FAIL_LEGACY_DWH=1
python3 r_legacy_ksh_dwh.py
```
* **Expected Output:** 
  ```
  YYYY-MM-DD HH:MM:SS Starting legacy_ksh_dwh export
  YYYY-MM-DD HH:MM:SS ERROR: legacy_ksh_dwh export failed
  ```
  Exits with code `1`. (Remember to `unset MOCK_FAIL_LEGACY_DWH` after testing).

### Airflow DAG Validation
1. Access the Airflow Web UI.
2. Locate the DAG `dw_exttest_legacy_dwh`.
3. Trigger the DAG manually.
4. Verify that the task `dw_exttest_legacy_dwh_task` completes with a `success` status.
5. Inspect the task logs to confirm the output matches the expected success logs.

---

## 7. Rollback Procedure

In the event of an issue during go-live, follow these steps to roll back to the legacy environment:

1. **Pause the Airflow DAG:**
   In the Airflow Web UI, toggle the switch for `dw_exttest_legacy_dwh` to **Off** (paused) to prevent any manual or external triggers.
2. **Re-enable UC4 Job:**
   In the UC4/Automic UI, ensure that the active flag for `DW.EXTTEST_LEGACY_DWH` is set to active (`active=1`) and any associated schedules are restored.
3. **Verify Legacy Execution:**
   Trigger a test run in UC4 to confirm that the legacy environment is executing the KornShell script on the legacy host (`dwhdwh2p`) successfully.