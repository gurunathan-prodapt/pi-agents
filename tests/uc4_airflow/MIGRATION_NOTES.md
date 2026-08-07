# Migration Notes: DW.DWH_ADM_JOB_MONITOR_START

This document details the migration of the UC4 Job Include (`JOBI`) utility `DW.DWH_ADM_JOB_MONITOR_START` to Apache Airflow on Google Cloud Platform (GCP).

---

## 1. Summary

The `DW.DWH_ADM_JOB_MONITOR_START` object is a reusable UC4 Job Include (`JOBI`) script block. In the legacy UC4 environment, it was embedded within parent jobs to perform standardized pre-execution monitoring and auditing. Specifically, it checked if a parent Job Plan was flagged for monitoring and, if so, registered the start of the job run in an active jobs registry.

This utility has been migrated to **Apache Airflow** as a reusable Python module and helper function, leveraging **Google BigQuery** as the backend metadata store. 

*   **Source Object Type:** UC4 Job Include (`JOBI`)
*   **Target Platform:** Apache Airflow (GCP / Composer)
*   **Target Backend Store:** Google BigQuery (replacing UC4 VARA tables)

---

## 2. Generated Artifacts

The migration process generated the following file:

| Target File Path | Language | Role / Description |
| :--- | :--- | :--- |
| `uc4_airflow/dw_dwh_adm_job_monitor_start.py` | Python | Shared library containing the core monitoring logic (`execute_job_monitor_start`), an Airflow Operator Mixin (`MonitoredOperatorMixin`), and a test/stub DAG (`dw_dwh_adm_job_monitor_start_test`) for validation. |

---

## 3. Key Design Decisions

### Reusable Python Module vs. Standalone DAG
In UC4, a `JOBI` is a passive code block that cannot run independently. To preserve this architecture, the logic was migrated as a **reusable Python function** rather than a standalone DAG. This function can be imported and executed dynamically by any Airflow DAG or task.

### Metadata Storage Migration (VARA to BigQuery)
*   **Legacy:** UC4 VARA objects `DW.DWH_MONITORED_JPS` (configuration) and `DW.DWH_RUNNING_JOBS` (active state registry) were used.
*   **Target:** Migrated to BigQuery tables `dwh_monitored_jps` and `dwh_running_jobs` respectively. BigQuery provides a highly available, centralized, and queryable metadata store suitable for cloud-native environments.

### Concurrency and DML Lock Mitigation
Directly updating or upserting a single row in a transactional table from hundreds of concurrent Airflow tasks can lead to BigQuery rate limits or DML transaction conflicts. 
*   **Decision:** The `dwh_running_jobs` table is designed as an **append-only log**. Every job start appends a new record. Downstream systems or monitoring dashboards should query this state using a deduplicating view:
    ```sql
    SELECT job_name, run_id, timestamp 
    FROM `your_project.your_dataset.dwh_running_jobs`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY timestamp DESC) = 1
    ```

### Dynamic Context Resolution
The legacy UC4 variables are mapped dynamically to Airflow's task execution context (`context` or `kwargs`):
*   `ADMJP` (Parent Job Plan) $\rightarrow$ `context['dag_run'].dag_id`
*   `ADMJOB` (Job Name) $\rightarrow$ `context['ti'].task_id`
*   `ADMNRJOB` (Run Number) $\rightarrow$ `context['dag_run'].run_id`

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling this utility, the following infrastructure and configuration steps must be completed in the target GCP environment.

### 1. BigQuery Schema Creation
Create the administrative monitoring tables in your designated BigQuery dataset.

```sql
-- 1. Table representing legacy VARA: DW.DWH_MONITORED_JPS
CREATE TABLE IF NOT EXISTS `your_project.monitoring_dataset.dwh_monitored_jps` (
    dag_id STRING OPTIONS(description="Airflow DAG ID or 'ALL'"),
    monitoring_enabled_flag STRING OPTIONS(description="'J' for enabled, 'N' for disabled")
);

-- 2. Table representing legacy VARA: DW.DWH_RUNNING_JOBS
CREATE TABLE IF NOT EXISTS `your_project.monitoring_dataset.dwh_running_jobs` (
    job_name STRING OPTIONS(description="Airflow Task ID"),
    run_id STRING OPTIONS(description="Airflow Run ID"),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Seed the monitoring configuration (Example: Enable monitoring for all DAGs)
INSERT INTO `your_project.monitoring_dataset.dwh_monitored_jps` (dag_id, monitoring_enabled_flag)
VALUES ('ALL', 'J');
```

### 2. IAM & Permissions
Ensure that the service account running the Airflow workers (e.g., Cloud Composer service account) has the following IAM roles on the BigQuery dataset:
*   `roles/bigquery.dataEditor` (to insert records into `dwh_running_jobs` and read `dwh_monitored_jps`)
*   `roles/bigquery.jobUser` (to run BigQuery query jobs)

### 3. Airflow Variables
Configure the following Airflow Variables in the Airflow UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-gcp-project-123` | The target GCP Project ID where BigQuery tables reside. |
| `BQ_DATASET` | `monitoring_dataset` | The BigQuery dataset containing the monitoring tables. |
| `BQ_CONNECTION_ID` | `google_cloud_default` | (Optional) The Airflow Connection ID for GCP. |

---

## 5. Known Gaps & Unresolved References

### Global Integration Pattern (Redesign Item B4)
Because Airflow does not have a native "Job Include" concept, developers must explicitly integrate this utility into their DAGs. There are three recommended integration patterns:

1.  **Explicit Task Call (Recommended for simplicity):**
    Call the helper function as the first task in a DAG using a `PythonOperator`.
2.  **Operator Mixin (Recommended for object-oriented pipelines):**
    Inherit from `MonitoredOperatorMixin` for custom operators to trigger the logic automatically during `pre_execute`.
3.  **Airflow Policy / Listener (Advanced):**
    Implement an Airflow Cluster Policy or a custom `on_execute_callback` in the default arguments of all DAGs to execute `execute_job_monitor_start` globally without modifying individual DAG files.

---

## 6. Validation

A test DAG (`dw_dwh_adm_job_monitor_start_test`) is included in the generated file to validate the end-to-end integration.

### Execution Test Steps
1.  Deploy `dw_dwh_adm_job_monitor_start.py` to your Airflow `dags/` folder.
2.  Ensure the BigQuery tables are created and seeded (see Section 4).
3.  Unpause the DAG `dw_dwh_adm_job_monitor_start_test` in the Airflow UI.
4.  Trigger the DAG manually.

### Verification of "Passing" Status
The run is successful if:
1.  The task `test_monitor_start` completes with a `SUCCESS` status.
2.  The Airflow task logs contain the following output (matching legacy logging specifications):
    ```text
    Checking monitoring status for DAG dw_dwh_adm_job_monitor_start_test in prod-gcp-project-123.monitoring_dataset.dwh_monitored_jps
    Added test_monitor_start with manual__2025-01-01T00:00:00+00:00
    Registering job start in prod-gcp-project-123.monitoring_dataset.dwh_running_jobs
    ```
3.  A query to the BigQuery table returns the registered run:
    ```sql
    SELECT * FROM `prod-gcp-project-123.monitoring_dataset.dwh_running_jobs` 
    WHERE job_name = 'test_monitor_start';
    ```

---

## 7. Rollback Procedure

If the monitoring utility causes performance degradation, database locks, or execution failures, follow these rollback steps:

### Option A: Soft Disable (No Code Changes)
To disable monitoring registration globally without redeploying code or pausing DAGs, update the configuration table in BigQuery:
```sql
UPDATE `your_project.monitoring_dataset.dwh_monitored_jps`
SET monitoring_enabled_flag = 'N'
WHERE dag_id = 'ALL';
```
This causes the utility to bypass the insert step and exit cleanly, preventing any writes to `dwh_running_jobs`.

### Option B: Code Reversion
If a hard rollback is required:
1.  Remove the `execute_job_monitor_start` calls or `MonitoredOperatorMixin` references from your DAG files.
2.  Redeploy the affected DAG files to the Airflow environment.