# Migration Notes: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This document details the migration of the legacy UC4 (Automic) job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Apache Airflow (Google Cloud Composer).

---

## 1. Summary

The legacy UC4 `JOBS_UNIX` object **`DW.DWH_DUMMY_ABSD_PLATO_TARIFE`** has been migrated to a native Apache Airflow DAG. 

In the source UC4 system, this job functioned as a utility or placeholder task (titled "dummy") that executed a basic diagnostic print statement (`:print Doing nothinig`). It did not perform any functional data processing or external system calls. 

Because the source extraction did not include a parent workflow (`JOBP`) or an active scheduling object (`EVNT_TIME`), this job has been migrated as a standalone, externally triggered Airflow DAG on **Google Cloud Composer**.

---

## 2. Generated Artifacts

The migration process generated the following file:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` | Airflow DAG Definition | Python script defining the DAG `dw_dwh_dummy_absd_plato_tarife` and its single execution task. |

---

## 3. Key Design Decisions

### Standalone DAG Structure
Since no parent workflow (`JOBP`) was supplied in this migration bundle, the job is modeled as a standalone DAG with `schedule=None`. This ensures it can be triggered manually or via an external orchestrator (such as a `TriggerDagRunOperator` or `ExternalTaskSensor` once its parent workflow is migrated).

### Operator Selection
Although the task performs no functional work, a **`BashOperator`** executing `echo 'Doing nothinig'` was chosen over an `EmptyOperator`. This decision preserves the exact operational logging behavior of the legacy UC4 script (`:print Doing nothinig`), ensuring that operators monitoring execution logs see parity between the old and new systems.

### Configuration Standardization
The DAG incorporates standard enterprise Airflow configurations:
* **`max_active_runs=1`**: Prevents concurrent execution runs.
* **`catchup=False`**: Disables backfilling.
* **Global Variables**: Dynamically retrieves `GCP_PROJECT` and `GCP_REGION` from Airflow Variables to maintain environment-agnostic deployment standards, even though this specific dummy task does not currently interact with GCP resources.

---

## 4. Manual Steps Before Go-Live

Before deploying this DAG to production, the following setup steps must be completed:

### 1. Airflow Variables
Ensure the following global Airflow variables are configured in the target Cloud Composer environment:
* `GCP_PROJECT`: The ID of your Google Cloud Project.
* `GCP_REGION`: The default region for your GCP resources.

### 2. IAM & Permissions
Because this task runs a basic local `echo` command via the `BashOperator`, no special service account permissions, external connection strings, or secrets are required. The standard Cloud Composer worker service account permissions are sufficient.

### 3. Downstream Integration
The legacy metadata indicates that this job is a dependency for the workflow **`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`**. 
* Once `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is migrated to Airflow, you must manually link these DAGs.
* This can be achieved by adding an `ExternalTaskSensor` in the downstream DAG or using a `TriggerDagRunOperator` at the end of this DAG.

---

## 5. Known Gaps & Unresolved References

### Unmigrated Downstream Workflow
* **Gap**: The downstream consumer workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is currently **not migrated**.
* **Impact**: End-to-end integration testing and automated scheduling cannot be finalized.
* **Resolution**: This DAG must remain on `schedule=None` and be triggered manually for testing until the downstream workflow is deployed.

### Redesign (B4) Recommendation
* **Note**: Dummy tasks are often used in legacy systems like UC4 to bypass scheduling limitations or act as synchronization joins. 
* **Recommendation**: During the migration of the parent workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`, evaluate whether this dummy task can be entirely deprecated or replaced with a native Airflow `EmptyOperator` directly inside the parent DAG, eliminating the overhead of managing a separate standalone DAG.

---

## 6. Validation

To validate the migrated DAG, perform the following steps:

### 1. DAG Syntax & Parsing Test
Run a local Python compilation check to ensure there are no syntax or import errors:
```bash
python3 uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py
```
* **Passing Criteria**: The command exits with code `0` and outputs no errors.

### 2. Airflow Local Task Execution Test
Test-run the specific task locally using the Airflow CLI:
```bash
airflow tasks test dw_dwh_dummy_absd_plato_tarife dw_dwh_dummy_absd_plato_tarife_task 2023-01-01
```
* **Passing Criteria**: The task execution completes successfully (`State: SUCCESS`) and the execution logs display:
  ```text
  Running command: ['bash', '-c', "echo 'Doing nothinig'"]
  Output:
  Doing nothinig
  Command exited with return code 0
  ```

---

## 7. Rollback Procedure

If issues occur post-deployment, follow these steps to roll back:

1. **Delete the DAG File**: Remove the Python file from the Cloud Composer GCS DAGs bucket:
   ```bash
   gsutil rm gs://<your-composer-dag-bucket>/dags/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py
   ```
2. **Verify Removal**: Log in to the Airflow Web UI and verify that the DAG `dw_dwh_dummy_absd_plato_tarife` is no longer listed (it may take up to 2 minutes for the webserver to refresh).
3. **Legacy Fallback**: If necessary, re-enable the legacy UC4 job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` (set `active=1`) to resume legacy operations.