# MIGRATION NOTES

**Job Path:** `Shared Files — folder1_uc4_ksh_abinitio/vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/ADMIN/DW.DWH_ADM_JOB_MONITOR`  
**Target Platform:** Google Cloud Platform (GCP) / Cloud Composer (Apache Airflow) / Cloud SQL (PostgreSQL)

---

## 1. Summary

This migration covers the transition of the UC4 Job Include (`JOBI`) scripts **`DW.DWH_ADM_JOB_MONITOR_START`** and **`DW.DWH_ADM_JOB_MONITOR_END`** to Apache Airflow on GCP. 

In the legacy UC4 environment, these scripts functioned as global preprocessing and postprocessing hooks. They registered job execution states, checked active monitoring variables, and logged business keys (`&DWH_JOB_KENNUNG`) to central tracking variables. 

On the target platform, these utilities have been migrated into modular, reusable Python functions and dedicated Airflow helper DAGs. They interface with a central PostgreSQL metadata database (simulating the legacy `DW.DWH_RUNNING_JOBS` table) and Airflow Variables (simulating `DW.DWH_MONITORED_JPS` and `DW.DWH_ADM_JOB_MONITOR_JOBKENNUNG_VAR`).

---

## 2. Generated Artifacts

The migration process generated three core files to be deployed to the Airflow environment:

| Target File Path | Language | Role / Description |
| :--- | :--- | :--- |
| `dags/utils/job_monitor_utils.py` | Python | **Core Logic Library:** Contains the functional Python logic for both start and end monitoring routines. Handles database connections, Airflow Variable lookups, and logging. |
| `dags/utils/dw_dwh_adm_job_monitor_start.py` | Python (Airflow DAG) | **Startup Utility DAG:** Wraps the preprocessing startup check and audit logging logic into a runnable Airflow task. |
| `dags/utils/dw_dwh_adm_job_monitor_end.py` | Python (Airflow DAG) | **Cleanup Utility DAG:** Wraps the postprocessing cleanup, state registry updates, and audit logging logic into a runnable Airflow task. |

---

## 3. Key Design Decisions

### Modularization of Shared Logic
* **Decision:** Extract the core execution logic into a shared utility file (`job_monitor_utils.py`) instead of duplicating code inside individual DAG files.
* **Reasoning:** This allows other DAGs to import and execute the monitoring logic directly as Python functions or tasks without needing to trigger external helper DAGs, reducing scheduler overhead.

### State Tracking & Database Storage
* **Decision:** Map the legacy dynamic variable `DW.DWH_RUNNING_JOBS` to a physical table (`dwh_running_jobs`) in a PostgreSQL metadata database accessed via `PostgresHook`.
* **Reasoning:** In UC4, global variables are stored in the automation engine database. In Airflow, using global Airflow Variables (`Variable.set`) for high-frequency, parallel writes causes database lock contention. A dedicated relational database table handles concurrent writes efficiently.

### Airflow Variable for Configuration
* **Decision:** Map the static UC4 variable `DW.DWH_MONITORED_JPS` to a JSON-based Airflow Variable (`dwh_monitored_dags`).
* **Reasoning:** This variable is read-heavy and rarely updated, making it an ideal candidate for Airflow's native Variable store.

### Verbatim Output Preservation
* **Decision:** Retain the exact German logging statements and dynamic string literals from the source XML.
* **Reasoning:** Ensures operational continuity and compatibility with legacy log-parsing scripts.
  * *Start Script:* `Added {task_id} with {run_id}`
  * *End Script:* `Jobkennung {dwh_job_kennung} eingetragen für {task_id}`

---

## 4. Manual Steps Before Go-Live

### Schema & Dataset Creation
Execute the following DDL on the target PostgreSQL metadata database to create the tracking table:

```sql
CREATE TABLE IF NOT EXISTS dwh_running_jobs (
    job_name VARCHAR(255) PRIMARY KEY,
    run_number VARCHAR(255) NOT NULL,
    registration_timestamp TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL
);
```

### IAM & Permissions
* Ensure the Cloud Composer service account has network access and credentials to connect to the target Cloud SQL PostgreSQL instance.
* Ensure the service account has the `roles/composer.worker` role to read and write Airflow Variables.

### Connection Strings & Secrets
Create an Airflow Connection for the metadata database:
* **Connection ID:** `metadata_audit_db` (or the value configured in the Airflow Variable `METADATA_AUDIT_DB_CONN`)
* **Connection Type:** `Postgres`
* **Host / Schema / Login / Password:** Set according to your Cloud SQL instance details.

### Airflow Variables Configuration
Import the following Airflow Variables via the Airflow UI (**Admin -> Variables**) or CLI:

1. **`dwh_monitored_dags`** (JSON map of monitored pipelines):
   ```json
   {
     "ALL": "J",
     "dw_example_dag": "J",
     "dw_unmonitored_dag": "N"
   }
   ```
2. **`GCP_PROJECT`**: `your-gcp-project-id`
3. **`METADATA_AUDIT_DB_CONN`**: `metadata_audit_db`

### Scheduling
These utility DAGs are configured with `schedule_interval=None`. They should not be scheduled independently. Instead, integrate them into parent pipelines as tasks or trigger them dynamically.

---

## 5. Known Gaps & Unresolved References

### Unmigrated Downstream Consumers (B4 Redesign Items)
The following downstream consumer pipelines depend on these monitoring routines but have **not yet been migrated**:
* `DW.BERT_AUSD_BP_TA_TARIFOPTION`
* `DW.DWH_ABPZ_KKM_AIL_AGENT`
* `DW.DWH_OAIS_EX_PPES_CUBES`

> [!WARNING]  
> **Action Required:** Once these downstream pipelines are migrated to Airflow, their DAG definitions must be updated to import and call `register_job_monitoring_start_logic` and `register_job_monitoring_end_logic` at their respective start and end phases.

---

## 6. Validation

### How to Run the Tests
1. **Unit Testing:** Execute local Python tests to verify the logic in `job_monitor_utils.py` using mocked Airflow contexts and database connections.
2. **Manual Trigger:** Trigger the `dw_dwh_adm_job_monitor_start` and `dw_dwh_adm_job_monitor_end` DAGs manually from the Airflow UI with a custom conf payload:
   ```json
   {
     "dwh_job_kennung": "TEST_KENNUNG_123"
   }
   ```

### What "Passing" Means
* **Start Script Validation:**
  * If the triggered DAG is in the `dwh_monitored_dags` variable with a value of `"J"`, a row must be successfully inserted/updated in the `dwh_running_jobs` table.
  * The task log must output: `Added register_job_start with <run_id>`.
  * If the DAG is not monitored, the task must raise an `AirflowSkipException` and gracefully skip.
* **End Script Validation:**
  * The task log must output: `Jobkennung TEST_KENNUNG_123 eingetragen für log_and_register_job_end`.
  * An Airflow Variable named `dw_dwh_adm_job_monitor_jobkennung_var_log_and_register_job_end` must be created or updated with the value `TEST_KENNUNG_123`.

---

## 7. Rollback Procedure

In the event of a deployment failure or critical issue:

1. **Pause Utility DAGs:** Pause the `dw_dwh_adm_job_monitor_start` and `dw_dwh_adm_job_monitor_end` DAGs in the Airflow UI.
2. **Revert Code:** Revert the git commit that deployed the files:
   * `dags/utils/job_monitor_utils.py`
   * `dags/utils/dw_dwh_adm_job_monitor_start.py`
   * `dags/utils/dw_dwh_adm_job_monitor_end.py`
3. **Clean Up Variables:** Delete any temporary test variables created in the Airflow Variable store (e.g., `dw_dwh_adm_job_monitor_jobkennung_var_*`).
4. **Database Rollback (Optional):** If necessary, truncate or drop the tracking table in PostgreSQL:
   ```sql
   DROP TABLE IF EXISTS dwh_running_jobs;
   ```