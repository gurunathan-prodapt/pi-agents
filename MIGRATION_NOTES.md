# MIGRATION_NOTES.md — Job: `d_alis_spaufruf_p0.sql`

This document provides comprehensive migration notes for transitioning the legacy Oracle SQL*Plus wrapper script `d_alis_spaufruf_p0.sql` to a modern, cloud-native orchestration workflow on Google Cloud Platform.

---

## 1. Summary

### Legacy Overview
The legacy script `d_alis_spaufruf_p0.sql` is an Oracle SQL*Plus wrapper script. Its primary responsibilities are:
1. Initializing environment settings and global variables by executing a shared script (`d_alis_init.sql`).
2. Setting up transactional error handling (`WHENEVER OSERROR EXIT FAILURE ROLLBACK;`).
3. Executing a parameterized stored procedure passed dynamically via a positional command-line argument (`EXEC &1;`).
4. Committing the transaction and exiting cleanly.

### Target Platform
The legacy logic has been migrated to **Google Cloud Platform (GCP)** using the **`UC4+KSH+SQL_SIMPLE`** high-confidence migration pattern:
*   **Orchestration**: **Cloud Composer (Apache Airflow)** manages the execution flow, handles dynamic parameters, and schedules runs.
*   **Execution Engine**: **BigQuery** executes the target logic natively via SQL `CALL` statements targeting BigQuery Stored Procedures.

---

## 2. Generated Artifacts

The migration process has produced the following target file:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `dags/d_alis_spaufruf_p0.py` | **Airflow DAG** | Python script defining the Cloud Composer workflow. It dynamically resolves execution parameters, builds the BigQuery `CALL` statement, and executes the target stored procedure. |

---

## 3. Key Design Decisions

### Dynamic Parameterization via Jinja Templating
*   **Decision**: Use Airflow's native Jinja templating engine combined with `dag_run.conf` to handle dynamic parameters.
*   **Reasoning**: The legacy script relied on SQL*Plus positional parameters (`&1`). To preserve this dynamic execution capability without hardcoding procedure names, the Airflow DAG reads parameters (such as `project_id`, `dataset_id`, `procedure_name`, and `procedure_param`) directly from the runtime configuration payload (`dag_run.conf`).
*   **Trade-off**: While dynamic execution reduces the number of static DAG files, it shifts the responsibility of validating input parameters to the runtime execution phase.

### Modular Operator Construction
*   **Decision**: Encapsulate the SQL generation and operator instantiation into reusable Python helper functions (`build_stored_procedure_query` and `create_bigquery_sp_operator`).
*   **Reasoning**: This modular structure isolates the core execution logic from the DAG definition block. It simplifies future maintenance, unit testing, and potential integration of pre-execution validation tasks.

### Transactional Safety and Session State
*   **Decision**: Delegate transaction management to BigQuery's native scripting capabilities (`BEGIN TRANSACTION`, `COMMIT TRANSACTION`, `EXCEPTION` blocks) within the target stored procedures, rather than handling them at the orchestration layer.
*   **Reasoning**: BigQuery is stateless across separate connection sessions. Replicating Oracle's session-level transaction states inside an Airflow operator is not feasible. Native BigQuery procedural transactions ensure atomic execution.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated DAG in production, complete the following administrative and configuration tasks:

### 1. Schema & Dataset Creation
*   Ensure that the target BigQuery dataset (e.g., `your_dataset_id`) exists in the designated GCP project.
*   Deploy the migrated target stored procedures to this dataset so they are available for the `CALL` statement.

### 2. IAM & Permissions
*   The Cloud Composer service account (worker identity) must be granted the following IAM roles on the target BigQuery datasets and projects:
    *   `roles/bigquery.jobUser` (to run query jobs).
    *   `roles/bigquery.dataViewer` / `roles/bigquery.dataEditor` (depending on whether the stored procedures perform read or write operations).

### 3. Airflow Connections & Variables
Configure the following variables in the Airflow Web UI (**Admin -> Variables**):
*   `gcp_project_id`: The target GCP project ID where BigQuery jobs will run.
*   `bq_default_dataset`: The default BigQuery dataset containing the stored procedures.
*   `gcp_connection_id`: The Airflow Connection ID for Google Cloud (defaults to `google_cloud_default`).

### 4. Scheduling & Triggering
*   The DAG is configured with `schedule_interval=None` to match the legacy on-demand execution model.
*   If this job needs to be scheduled, update the `schedule_interval` parameter in `dags/d_alis_spaufruf_p0.py` or trigger it via an upstream DAG.

---

## 5. Known Gaps & Unresolved References

### Shared Initializer (`d_alis_init.sql`)
*   **Gap**: The legacy script calls `START d_alis_init.sql`. This initializer may set session-level variables, environment configurations, or logging tables in Oracle.
*   **Resolution**: Any global variables or logging configurations established in `d_alis_init.sql` must be manually migrated. If they are required for execution, they should be passed as Airflow Variables or handled within the BigQuery stored procedure body.

### Dynamic Multi-Argument Procedures
*   **Gap**: The current DAG template assumes a single-parameter stored procedure call: `CALL \`project.dataset.procedure\`('parameter')`.
*   **Resolution**: If a migrated legacy procedure requires multiple arguments or different data types, the SQL generation block in `build_stored_procedure_query()` must be updated or overridden via the `dag_run.conf` payload to format the arguments correctly.

---

## 6. Validation

To validate the migration, perform the following testing steps:

### 1. Airflow DAG Parsing Test
Verify that the DAG is syntactically correct and can be parsed by Airflow without errors:
```bash
python dags/d_alis_spaufruf_p0.py
```
*(An empty output indicates a successful parse with no syntax errors).*

### 2. Manual Trigger Test (Dry Run)
Trigger the DAG manually from the Airflow UI or CLI, passing a test payload:
```bash
gcloud composer environments run <your-composer-env> \
    --location <your-region> \
    dags trigger -- d_al_is_spaufruf_p0_dag \
    --conf '{"project_id": "my-dev-project", "dataset_id": "my_dataset", "procedure_name": "my_test_procedure", "procedure_param": "test_value"}'
```

### 3. Definition of "Passing"
The validation is considered successful ("passing") when:
*   The Airflow task `execute_stored_procedure_task` transitions to a `SUCCESS` state.
*   The BigQuery Job History confirms that the query `CALL \`my-dev-project.my_dataset.my_test_procedure\`('test_value')` executed and completed successfully.
*   Any data modifications or side effects expected from the target stored procedure are verified in the target BigQuery tables.

---

## 7. Rollback Procedure

In the event of a production failure or unexpected behavior, execute the following rollback steps:

1.  **Pause the DAG**: Immediately pause the `d_al_is_spaufruf_p0_dag` in the Airflow UI to prevent further executions.
2.  **Identify Active Executions**: Check the Airflow task instances for any currently running tasks and terminate them if necessary.
3.  **Revert to Legacy Execution**:
    *   If the legacy Oracle environment is still active, route upstream triggers back to the legacy SQL*Plus execution path (`sqlplus user/pass @d_alis_spaufruf_p0.sql <procedure_name>`).
4.  **Investigate Logs**: Analyze the Cloud Composer task logs and BigQuery Job logs to determine the root cause of the failure (e.g., permission issues, schema mismatches, or parameter parsing errors).