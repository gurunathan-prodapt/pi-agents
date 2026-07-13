# MIGRATION_NOTES.md — Job: `d_alis_spaufruf_p0.sql`

This document provides comprehensive migration notes for transitioning the legacy Oracle SQL\*Plus wrapper script `d_alis_spaufruf_p0.sql` to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow) and BigQuery.

---

## 1. Summary

* **Source Component:** `d_alis_spaufruf_p0.sql` (Oracle SQL\*Plus Wrapper / PL/SQL execution script).
* **Target Platform:** Google Cloud Platform (GCP).
* **Target Services:** Cloud Composer (Apache Airflow) & BigQuery.
* **Migration Pattern:** UC4 + KSH + SQL_SIMPLE (orchestrated via Cloud Composer DAG executing dynamic BigQuery `CALL` statements).
* **Core Functionality:** The legacy script initialized session parameters (via `d_alis_init.sql`), enabled server output, executed a dynamically passed stored procedure (`&1`) with optional arguments (`&2`), and committed the transaction. The migrated solution replaces this wrapper with a parameterized Airflow DAG that dynamically invokes BigQuery Stored Procedures.

---

## 2. Generated Artifacts

The migration process yields the following target files:

| Target File Path | Language | Role |
| :--- | :--- | :--- |
| `dags/d_al_is_spaufruf_p0.py` | Python (Airflow DAG) | Orchestrates the execution of the target BigQuery stored procedure. It dynamically parses the procedure name and arguments from the DAG run configuration (`dag_run.conf`), mimicking the legacy SQL\*Plus positional parameters (`&1` and `&2`). |
| `definitions/d_alis_init.sqlx` *(or equivalent)* | Dataform / SQL | Placeholder for shared environment initialization logic. If manual analysis of the legacy `d_alis_init.sql` reveals actual DDL/DML or global state initialization (rather than just SQL\*Plus formatting), it must be compiled here. |

---

## 3. Key Design Decisions

### Dynamic Parameterization via Airflow `dag_run.conf`
* **Decision:** Instead of hardcoding specific stored procedure calls, the Airflow DAG uses Jinja templating to extract `sp_name` and `sp_args` from the runtime configuration (`dag_run.conf`).
* **Reasoning:** This preserves the highly dynamic nature of the legacy wrapper script, allowing upstream scheduling systems (like UC4/Automic) to trigger a single DAG while executing different underlying business logic.
* **Trade-off:** This introduces a dependency on structured JSON payloads during DAG triggering. If an upstream system triggers the DAG without providing these parameters, it will fall back to safe defaults (`default_procedure` with no arguments), which must be monitored.

### Elimination of SQL\*Plus Session Settings
* **Decision:** Formatting settings such as `SET SERVEROUTPUT ON`, `SET PAGESIZE`, and `SET HEADING` have been completely omitted.
* **Reasoning:** BigQuery is a serverless data warehouse and does not support or require terminal-formatting session commands. Logging is handled natively via Google Cloud Logging.

### Transaction Management
* **Decision:** Explicit `COMMIT` and `ROLLBACK` commands from the wrapper script are omitted in the Airflow task.
* **Reasoning:** BigQuery automatically commits transactional statements executed within stored procedures. Any multi-statement transactional atomicity requirements must be handled internally within the migrated BigQuery Stored Procedures using `BEGIN TRANSACTION` and `EXCEPTION` blocks.

---

## 4. Manual Steps Before Go-Live

### 1. Schema & Dataset Creation
Ensure that the target BigQuery dataset exists in your designated GCP project:
```bash
bq mk --dataset --location=EU your_project_id:your_dataset
```

### 2. IAM & Permissions
The Service Account running the Cloud Composer workers must have the following IAM roles:
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level to run queries.
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target dataset (`your_dataset`) to execute procedures that modify tables.

### 3. Airflow Variables Configuration
Before executing the DAG, configure the following Airflow Variables in the Cloud Composer UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `gcp_conn_id` | `google_cloud_default` | The Airflow connection ID used to connect to GCP. |
| `gcp_project_id` | `prod-gcp-project-123` | The target GCP Project ID. |
| `bq_dataset` | `d_alis_prod` | The target BigQuery dataset containing the stored procedures. |
| `dag_default_retries` | `1` | Number of retries for failed tasks. |
| `dag_default_retry_delay_mins` | `5` | Delay between retries in minutes. |

### 4. Scheduling & Upstream Integration
If this job is triggered by an external scheduler (e.g., UC4/Automic):
1. Configure the UC4 job to call the Google Cloud Composer API to trigger the DAG.
2. Pass the execution parameters in the POST request body as a JSON payload:
   ```json
   {
     "conf": {
       "sp_name": "your_actual_stored_procedure",
       "sp_args": "123, 'dynamic_string_value'"
     }
   }
   ```

---

## 5. Known Gaps & Unresolved References

### 1. Legacy Initialization Script (`d_alis_init.sql`)
* **Status:** **UNRESOLVED (SOURCE NOT FOUND)**.
* **Action Required:** Locate the legacy file at `$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_init.sql`. 
  * If it contains only SQL\*Plus formatting commands (e.g., `SET FEEDBACK OFF`), **no action is required**.
  * If it initializes global session variables or temporary tables, those must be manually refactored into BigQuery session variables or handled within the individual migrated stored procedures.

### 2. Target Stored Procedures (`&1`)
* **Status:** **PENDING MIGRATION**.
* **Action Required:** The wrapper script itself does not contain business logic. Every Oracle PL/SQL stored procedure that was historically executed via this wrapper must be individually migrated to BigQuery syntax (`CREATE OR REPLACE PROCEDURE`) and deployed to the target dataset before triggering this DAG.

---

## 6. Validation

### How to Run the Test
You can trigger a manual test run of the DAG using the Airflow CLI or the Cloud Composer UI.

#### Via Airflow CLI (Cloud Composer gcloud command):
```bash
gcloud composer environments run YOUR_ENV_NAME \
    --location YOUR_REGION \
    dags trigger -- d_al_is_spaufruf_p0 \
    --conf '{"sp_name": "test_procedure", "sp_args": "42, '\''test_param'\''"}'
```

### What "Passing" Means
1. **Airflow Task Status:** The `execute_migrated_stored_procedure` task transitions to `SUCCESS`.
2. **BigQuery Job Logs:** In the BigQuery Console, navigate to **Project History**. You should see a successfully completed `CALL` statement:
   ```sql
   CALL `your-gcp-project-id.your_dataset.test_procedure`(42, 'test_param');
   ```
3. **Data Integrity:** Verify that any tables modified by the target stored procedure (`test_procedure`) reflect the expected changes.

---

## 7. Rollback Procedure

In the event of a failure or unexpected behavior post-deployment:

1. **Pause the Airflow DAG:**
   Go to the Airflow UI and toggle the switch for `d_al_is_spaufruf_p0` to **Off** to prevent any automated or external triggers from executing.
2. **Revert Upstream Scheduler:**
   If UC4/Automic was switched to trigger the Cloud Composer DAG, revert the UC4 task definition to point back to the legacy KSH/SQL\*Plus wrapper execution path:
   ```bash
   # Legacy execution command example
   sqlplus username/password@database @d_alis_spaufruf_p0.sql target_procedure_name "arguments"
   ```
3. **Database State Cleanup:**
   If the failed execution left target tables in an inconsistent state, run manual cleanup scripts or restore the affected BigQuery tables to a point-in-time prior to the failure using BigQuery's FOR SYSTEM TIME AS OF feature:
   ```sql
   CREATE OR REPLACE TABLE `your_project.your_dataset.your_table` AS
   SELECT * FROM `your_project.your_dataset.your_table`
   FOR SYSTEM TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```