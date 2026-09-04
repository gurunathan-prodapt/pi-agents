# Migration Notes: DW.DWH_ADM_JOB_MONITOR_START

This document details the migration of the UC4 Job Include (`JOBI`) object `DW.DWH_ADM_JOB_MONITOR_START` to Google Cloud Composer (Apache Airflow).

---

## 1. Summary

The UC4 Job Include (`JOBI`) object **`DW.DWH_ADM_JOB_MONITOR_START`** has been migrated to a reusable Python module and template DAG within Google Cloud Composer. 

In UC4, a `JOBI` object is a non-executable, reusable script block designed to be included by reference in parent job scripts. This specific include block performs startup registration and monitoring checks:
1. It checks if the parent Job Plan (`JOBP`) is registered for monitoring in a global configuration container (`DW.DWH_MONITORED_JPS`).
2. If monitoring is enabled for the parent workflow, it registers the running job and its run number into an active tracking registry (`DW.DWH_RUNNING_JOBS`).

To preserve this reusable architecture in Airflow, the logic has been refactored into a Python helper function that dynamically extracts execution context (DAG ID, Task ID, Run ID) and interacts with Airflow Variables acting as the global state registries.

---

## 2. Generated Artifacts

The migration process generated the following file:

*   **`uc4_airflow/DW_DWH_ADM_JOB_MONITOR_START.py`**
    *   **Role:** Contains the core Python helper function `dwh_adm_job_monitor_start` which implements the ported UC4 monitoring logic.
    *   **Template DAG:** Includes a placeholder DAG (`dw_dwh_adm_job_monitor_start_template`) to demonstrate how parent DAGs should import and invoke this monitoring task at startup.

---

## 3. Key Design Decisions

### Reusable Python Helper vs. Standalone DAG
Because UC4 `JOBI` objects cannot execute independently, migrating this as a standalone DAG would be non-functional. Instead, the logic is encapsulated in a Python function (`dwh_adm_job_monitor_start`) designed to be imported by any DAG requiring monitoring.

### State Mapping (UC4 VARA to Airflow Variables)
*   **`DW.DWH_MONITORED_JPS`** (UC4 Variable) $\rightarrow$ Mapped to the Airflow Variable `DW_DWH_MONITORED_JPS` (JSON format).
*   **`DW.DWH_RUNNING_JOBS`** (UC4 Variable) $\rightarrow$ Mapped to the Airflow Variable `DW_DWH_RUNNING_JOBS` (JSON format).

### Dynamic Context Extraction
UC4 system functions are mapped directly to Airflow's task instance context:
*   `SYS_ACT_JPNAME()` (Parent Job Plan) $\rightarrow$ `context['dag'].dag_id`
*   `SYS_ACT_JOBNAME()` (Active Job) $\rightarrow$ `context['task'].task_id`
*   `SYS_ACT_JOBNR()` (Run Number) $\rightarrow$ `context['dag_run'].run_id`

### Trade-offs
Using Airflow Variables for dynamic state tracking (`DW_DWH_RUNNING_JOBS`) is simple and requires no external database setup. However, Airflow Variables are stored in the Airflow metadata database. Frequent writes from highly concurrent DAGs may cause database lock contention. (See *Section 5: Known Gaps* for redesign recommendations).

---

## 4. Manual Steps Before Go-Live

Before deploying and running any DAGs that utilize this monitoring helper, the following manual setup steps must be completed in the target Airflow environment:

### 1. Airflow Variables Creation
You must define the following Airflow Variables via the Airflow UI (**Admin -> Variables**) or the CLI:

*   **`DW_DWH_MONITORED_JPS`**: A JSON-formatted lookup dictionary or list defining which DAGs should be monitored.
    *   *Example Value (Monitor All):*
        ```json
        {
          "ALL": "J"
        }
        ```
    *   *Example Value (Monitor Specific DAGs):*
        ```json
        {
          "dw_dwh_adm_job_monitor_start_template": "J",
          "some_other_dag": "N"
        }
        ```
*   **`DW_DWH_RUNNING_JOBS`**: Initialize this as an empty JSON object.
    *   *Value:*
        ```json
        {}
        ```
*   **Global Environment Placeholders** (referenced in the script header):
    *   `GCP_PROJECT`: Your Google Cloud Project ID.
    *   `DATAPROC_REGION`: Target Dataproc region.
    *   `DATAPROC_CLUSTER`: Target Dataproc cluster name.
    *   `GCS_BUCKET`: Target GCS bucket name.

### 2. IAM & Permissions
Ensure that the Cloud Composer Service Account has sufficient permissions to read and write to the Airflow metadata database (granted by default to Composer worker roles).

### 3. Code Deployment
Copy `DW_DWH_ADM_JOB_MONITOR_START.py` into your Airflow environment's `dags/` folder. 
> **Note:** In a production environment, it is recommended to move the `dwh_adm_job_monitor_start` function into a shared utility folder (e.g., `plugins/utils/`) and import it into your DAGs.

---

## 5. Known Gaps & Unresolved References

### 1. Race Conditions on Variable Writes (Redesign B4 Item)
*   **Gap:** The function reads `DW_DWH_RUNNING_JOBS`, appends the current job, and writes it back. If multiple tasks execute this helper simultaneously, a race condition (last-write-wins) will occur, causing lost monitoring records.
*   **Redesign Recommendation:** For production environments, replace the Airflow Variable-based registry with a transactional database table (e.g., a BigQuery metadata table or a Cloud SQL instance) using row-level locking or append-only inserts.

### 2. Missing Parent Context
*   The original UC4 bundle did not contain the parent workflows (`JOBP`) that call this include. The template DAG provided is a structural placeholder and must be integrated into actual migrated workflows.

---

## 6. Validation

To validate the migration of this include logic:

1.  Navigate to the Airflow UI and unpause the `dw_dwh_adm_job_monitor_start_template` DAG.
2.  Trigger the DAG manually.
3.  Verify that the task `dwh_adm_job_monitor_start` completes with a **Success** status.
4.  Inspect the task logs. They must contain the following literal strings (matching the original German and English logging requirements):
    *   `Job dwh_adm_job_monitor_start mit RNR <RUN_ID> gestartet aus dw_dwh_adm_job_monitor_start_template`
    *   `Added dwh_adm_job_monitor_start with <RUN_ID>`
5.  Navigate to **Admin -> Variables** and verify that the `DW_DWH_RUNNING_JOBS` variable has been updated to include the task run:
    ```json
    {
      "dwh_adm_job_monitor_start": "manual__2023-..."
    }
    ```

---

## 7. Rollback Procedure

In the event of a failure or unexpected behavior:

1.  **Pause the DAGs:** Pause the template DAG and any parent DAGs utilizing the `dwh_adm_job_monitor_start` helper.
2.  **Clear Active Registry:** Reset the `DW_DWH_RUNNING_JOBS` Airflow Variable to an empty JSON object `{}` to prevent stale state propagation.
3.  **Remove Code:** Delete the `DW_DWH_ADM_JOB_MONITOR_START.py` file from the Airflow `dags/` directory (or revert the import statements in parent DAGs).