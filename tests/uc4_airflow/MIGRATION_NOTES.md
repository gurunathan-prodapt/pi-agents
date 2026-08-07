# Migration Notes: DW.DWH_ADM_JOB_MONITOR_START

This document details the migration of the UC4 Include Script (`JOBI`) object `DW.DWH_ADM_JOB_MONITOR_START` to Apache Airflow (Google Cloud Composer).

---

## 1. Summary
The `DW.DWH_ADM_JOB_MONITOR_START` UC4 object is a reusable Include Script (`JOBI`) designed to initialize job monitoring and telemetry. It captures metadata about the running job and its parent workflow, checks if the parent workflow is configured for monitoring, and registers the active execution in a shared running jobs registry.

This utility has been migrated from **UC4/Automic** to **Apache Airflow** on **Google Cloud Composer**. Because a `JOBI` object is not a standalone executable workflow, the logic has been encapsulated into a reusable Python helper function within an Airflow DAG structure, ready to be integrated into parent DAG lifecycles.

---

## 2. Generated Artifacts
The migration process generated the following file:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `uc4_airflow/dw_dwh_adm_job_monitor_start.py` | Airflow DAG & Helper Module | Contains the core monitoring logic mapped to Airflow's execution context, wrapped in a Python callable task. |

---

## 3. Key Design Decisions

### JOBI to Reusable Python Logic
In UC4, `JOBI` scripts are dynamically included at runtime. In Airflow, the equivalent best practice is to write a shared Python helper function. While the generated artifact is structured as a standalone DAG for testing and representation, the core function `execute_job_monitor_start` is designed to be imported and executed as a task or callback within other production DAGs.

### Context Mapping
UC4 scheduler-set variables have been mapped directly to Airflow’s native task instance context:
* **`SYS_ACT_JPNAME()` (Parent JobPlan)** $\rightarrow$ `context['dag'].dag_id`
* **`SYS_ACT_JOBNAME()` (Job Name)** $\rightarrow$ `context['task_instance'].task_id`
* **`SYS_ACT_JOBNR()` (Run Number)** $\rightarrow$ `context['run_id']`

### State Management Trade-offs
The legacy UC4 script reads configuration from `DW.DWH_MONITORED_JPS` and writes state to `DW.DWH_RUNNING_JOBS` using `PUT_VAR`. 
* **Chosen Approach**: The migrated code uses Airflow Variables (`DW_DWH_MONITORED_JPS` and `DW_DWH_RUNNING_JOBS`) storing JSON payloads to mimic this behavior.
* **Trade-off**: While using Airflow Variables is a direct functional translation, frequent concurrent updates (`Variable.set`) from multiple running DAGs can cause database lock contention in the Airflow metadata database. (See Section 5 for the recommended production redesign).

---

## 4. Manual Steps Before Go-Live

### 1. Seed Airflow Variables
You must create and seed the following Airflow Variables via the Airflow UI (**Admin -> Variables**) or CLI before executing the task:

* **`DW_DWH_MONITORED_JPS`**: A JSON dictionary defining which DAGs are monitored.
  ```json
  {
    "dw_dwh_adm_job_monitor_start": "J",
    "example_monitored_dag": "J",
    "ALL": "N"
  }
  ```
* **`DW_DWH_RUNNING_JOBS`**: An empty JSON dictionary to initialize the running jobs registry.
  ```json
  {}
  ```

### 2. IAM & Permissions
If you choose to implement the BigQuery metadata table redesign (recommended below), ensure that the Cloud Composer environment's service account has the **BigQuery Data Editor** (`roles/bigquery.dataEditor`) role on the target dataset.

### 3. Integration into Parent DAGs
Since this is a utility script, you must manually integrate this logic into your migrated production DAGs. This can be achieved by:
1. Importing `execute_job_monitor_start` from `dw_dwh_adm_job_monitor_start.py`.
2. Invoking it as the first task in your DAG, or registering it within an `on_execute_callback`.

---

## 5. Known Gaps & Unresolved References

### Redesign (B4) Item: High-Frequency Variable Updates
* **The Issue**: The current implementation updates the Airflow Variable `DW_DWH_RUNNING_JOBS` every time a monitored task starts. Under high concurrency, this will degrade Airflow metadata database performance.
* **Resolution**: For production environments, replace the Airflow Variable write logic with a direct insert into a centralized BigQuery logging table (e.g., `GCP_PROJECT.BQ_DATASET.dw_dwh_running_jobs`). 

### Missing Parent Context
The original UC4 extraction bundle did not contain the calling JobPlans (`JOBP`) or Jobs (`JOBS`). The developer must manually identify which migrated DAGs require this monitoring hook and insert the helper task accordingly.

---

## 6. Validation

### How to Run the Test
1. Upload `dw_dwh_adm_job_monitor_start.py` to your Airflow `dags/` folder.
2. Ensure the Airflow Variables `DW_DWH_MONITORED_JPS` and `DW_DWH_RUNNING_JOBS` are seeded.
3. Trigger the DAG `dw_dwh_adm_job_monitor_start` manually via the Airflow UI or CLI:
   ```bash
   airflow dags trigger dw_dwh_adm_job_monitor_start
   ```

### What "Passing" Looks Like
1. The task `dwh_adm_job_monitor_start_helper` completes with a `success` status.
2. Task logs display the registration output:
   ```text
   Added dwh_adm_job_monitor_start_helper with manual__2023-10-24T...
   ```
3. Inspecting the Airflow Variable `DW_DWH_RUNNING_JOBS` reveals the newly registered task:
   ```json
   {
     "dwh_adm_job_monitor_start_helper": "manual__2023-10-24T..."
   }
   ```

---

## 7. Rollback Procedure

If the migrated monitoring utility causes performance issues or failures in downstream DAGs:

1. **Disable Monitoring Triggers**: Set the active flag to `"N"` for the affected DAGs (or set `"ALL": "N"`) in the `DW_DWH_MONITORED_JPS` Airflow Variable. This bypasses the registration logic without requiring code changes.
2. **Remove DAG Integration**: If the helper was integrated as an explicit task or callback in other DAGs, revert those DAG files to their previous versions to remove the imports.
3. **Clear Registry**: Reset the `DW_DWH_RUNNING_JOBS` Airflow Variable to `{}` to clear any stale registration state.