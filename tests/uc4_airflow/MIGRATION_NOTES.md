# Migration Notes: DW.DWH_ADM_JOB_MONITOR_START

## 1. Summary
The UC4 Include (`JOBI`) utility object **`DW.DWH_ADM_JOB_MONITOR_START`** has been migrated to Apache Airflow on Google Cloud Platform (GCP). 

In the legacy UC4 environment, this Include object functioned as a reusable script block embedded dynamically within other executable jobs to register the start of a job run in a global registry. Because Include objects cannot execute independently, the asset has been refactored into a reusable Python module containing the core monitoring registration logic, accompanied by a stub Airflow DAG wrapper to facilitate standalone deployment, testing, and structural validation.

---

## 2. Generated Artifacts
The migration process generated the following file:

* **`uc4_airflow/dw_dwh_adm_job_monitor_start.py`**
  * **Role**: Contains both the core Python function `dwh_adm_job_monitor_start` (which interfaces with BigQuery to check monitoring status and register active runs) and a stub Airflow DAG definition (`dw_dwh_adm_job_monitor_start`) to allow independent validation of the logic.

---

## 3. Key Design Decisions

### Refactoring JOBI to a Reusable Python Module
UC4 `JOBI` objects are script templates merged into parent jobs at runtime. To preserve this reusability in Airflow, the logic was encapsulated into a standard Python function (`dwh_adm_job_monitor_start`). Any Airflow DAG requiring start-monitoring tracking can import this function and execute it via a `PythonOperator` or invoke it within an `on_execute_callback`.

### Mapping UC4 VARA Objects to BigQuery Metadata Tables
The legacy UC4 script relied on two Variable (`VARA`) objects for state management. These have been mapped to centralized Google Cloud BigQuery tables:
* `DW.DWH_MONITORED_JPS` $\rightarrow$ `dw_metadata.dwh_monitored_jps` (Configuration table)
* `DW.DWH_RUNNING_JOBS` $\rightarrow$ `dw_metadata.dwh_running_jobs` (Active registry table)

### Idempotent Registration via BigQuery MERGE
To prevent duplicate entries and handle task retries gracefully, the registration logic utilizes a BigQuery `MERGE` statement. If a job run ID already exists for a given job name, it is updated; otherwise, a new record is inserted.

### Exact Log Preservation
To ensure compatibility with legacy log parsers, operational dashboards, or audit tools, the exact character-for-character logging output from the UC4 script has been preserved in Python:
* `Job {admjob} mit RNR {admnrjob} gestartet aus {admjp}`
* `Added {admjob} with {admnrjob}`

---

## 4. Manual Steps Before Go-Live

### 1. Schema and Dataset Creation
You must provision the metadata tables in BigQuery before executing the code. Run the following DDL statements in your target GCP project:

```sql
-- Create the metadata dataset if it does not exist
CREATE SCHEMA IF NOT EXISTS dw_metadata;

-- Create the Monitored Job Plans configuration table
CREATE TABLE IF NOT EXISTS dw_metadata.dwh_monitored_jps (
    job_plan_name STRING OPTIONS(description="The DAG ID or parent job plan name"),
    monitoring_status STRING OPTIONS(description="'J' for active monitoring, 'N' for inactive")
);

-- Create the Running Jobs active registry table
CREATE TABLE IF NOT EXISTS dw_metadata.dwh_running_jobs (
    job_name STRING OPTIONS(description="The name of the running task or DAG"),
    run_id STRING OPTIONS(description="The unique Airflow Run ID")
);
```

### 2. IAM & Permissions
The service account running the Airflow workers must be granted the following IAM roles on the BigQuery dataset:
* `roles/bigquery.dataEditor` (to insert/update running job records)
* `roles/bigquery.jobUser` (to run the query jobs)

### 3. Airflow Variables
Configure the following Airflow Variables via the Airflow UI (**Admin -> Variables**) or CLI:
* `GCP_PROJECT`: The target Google Cloud Project ID hosting your BigQuery datasets.
* `BQ_DATASET`: (Optional) Defaults to `dw_metadata` if not specified.

### 4. Scheduling
The stub DAG `dw_dwh_adm_job_monitor_start` is configured with `schedule=None` and `is_paused_upon_creation=True`. It should remain paused in production, as it is designed to be called dynamically by other workflows.

---

## 5. Known Gaps & Unresolved References

### Integration with Parent DAGs (Redesign Item)
Because no parent workflows (`JOBP`) or calling jobs (`JOBS`) were supplied in the migration bundle, this utility currently exists in isolation. 
* **Action Required**: For every migrated DAG that requires start-monitoring, developers must manually import the `dwh_adm_job_monitor_start` function and add it as the initial task in the DAG.
  
  *Example Integration:*
  ```python
  from uc4_airflow.dw_dwh_adm_job_monitor_start import dwh_adm_job_monitor_start
  
  monitor_start = PythonOperator(
      task_id='monitor_start',
      python_callable=dwh_adm_job_monitor_start,
      provide_context=True,
  )
  ```

---

## 6. Validation

### Test Execution Procedure
To validate the migration logic using the generated stub DAG:

1. **Seed the Configuration Table**: Insert a test row into the BigQuery monitoring configuration table to enable monitoring for the stub DAG:
   ```sql
   INSERT INTO `dw_metadata.dwh_monitored_jps` (job_plan_name, monitoring_status)
   VALUES ('dw_dwh_adm_job_monitor_start', 'J');
   ```

2. **Trigger the DAG**: Unpause and manually trigger the `dw_dwh_adm_job_monitor_start` DAG via the Airflow UI or CLI:
   ```bash
   airflow dags unpause dw_dwh_adm_job_monitor_start
   airflow dags trigger dw_dwh_adm_job_monitor_start
   ```

3. **Verify Task Logs**: Inspect the logs of the task `dwh_adm_job_monitor_start_include`. A successful run must output the following lines:
   ```text
   Job dw_dwh_adm_job_monitor_start mit RNR manual__202X-XX-XX... gestartet aus dw_dwh_adm_job_monitor_start
   Added dw_dwh_adm_job_monitor_start with manual__202X-XX-XX...
   ```

4. **Verify BigQuery Registry**: Query the running jobs table to confirm the record was successfully written:
   ```sql
   SELECT * FROM `dw_metadata.dwh_running_jobs` 
   WHERE job_name = 'dw_dwh_adm_job_monitor_start';
   ```
   *Expected Result*: One row containing the job name and the corresponding Airflow Run ID.

---

## 7. Rollback Procedure

In the event of an issue or deployment failure, execute the following steps to roll back:

1. **Pause/Delete the DAG**: Pause the stub DAG in the Airflow UI or delete it via the CLI:
   ```bash
   airflow dags pause dw_dwh_adm_job_monitor_start
   ```
2. **Remove Artifacts**: Delete the generated Python file from your Airflow DAGs directory:
   ```bash
   rm ${AIRFLOW_HOME}/dags/uc4_airflow/dw_dwh_adm_job_monitor_start.py
   ```
3. **Revert Parent DAG Integrations**: If any parent DAGs were modified to import and call this module, revert those code changes to their previous state.
4. **Clean Up Metadata (Optional)**: If a clean state is desired, truncate the active running jobs registry in BigQuery:
   ```sql
   TRUNCATE TABLE `dw_metadata.dwh_running_jobs`;
   ```