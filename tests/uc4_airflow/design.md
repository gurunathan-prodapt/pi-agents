=== OBJECT: DW.DWH_ADM_JOB_MONITOR_START (JOBI) ===
active=None
title=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Migration Design Document: UC4 to Apache Airflow

## 1. Overview
UNCERTAIN: This extraction bundle contains a single UC4 Job Include (`JOBI`) utility object: `DW.DWH_ADM_JOB_MONITOR_START`. In UC4, a JOBI is a reusable script block embedded within other job definitions (such as `JOBS_UNIX` or `JOBS_WINDOWS`) to perform standardized pre-execution actions, such as registering the start of a process in an audit table, setting environment variables, or initializing monitoring states. Because no parent workflows (`JOBP`) or executable jobs (`JOBS`) were supplied in this extraction, this object represents a shared utility module rather than a standalone scheduled workflow.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---|---|
| `DW.DWH_ADM_JOB_MONITOR_START` | JOBI | None | Job Include (reusable monitoring/audit startup script block) |

## 3. Scheduling
No calendar-based or event-based schedules exist in this bundle, as no `EVNT_TIME` or schedule objects were provided. 
- **Trigger Source:** This object is a passive utility script. It is designed to be referenced inline by other UC4 jobs. It has no independent scheduling properties.
- **Airflow Schedule:** `schedule=None` (not applicable as a standalone DAG).

## 4. Airflow DAG Properties
No `JOBP` objects are present in this extraction bundle. Therefore, no primary production Airflow DAG is generated directly from this bundle. 

If a test or stub DAG is required to verify the helper logic, the properties would be as follows:

| Property | Value |
|---|---|
| `dag_id` | `dw_dwh_adm_job_monitor_start_test` |
| `schedule` | `None` |
| `start_date` | `datetime(2025, 1, 1)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `is_paused_upon_creation` | `True` |
| `default_args` | `{'owner': 'data_engineering', 'retries': 0}` |

## 5. Task Inventory
No `JOBP` task entries exist in this bundle. Therefore, no task instances are mapped to standard Airflow operators. 

## 6. Task Dependency Map
No task dependencies exist in this extraction bundle.

## 7. Sync / Concurrency Analysis
No workflow-level sync rows or concurrency locks are defined in this bundle.

## 8. Error Handling and Retry Strategy
Because a JOBI contains inline script code that runs inside a parent job's execution context, its error handling is traditionally governed by the parent task's wrapper. In Apache Airflow, this monitoring-start registration logic should be implemented either as an custom Operator class, a custom Airflow Listener, or a shared function called via `pre_execute` hooks or explicitly at the beginning of task execution (e.g., inside an `on_execute_callback`).

## 9. Parameter and Variable Mapping
No explicit variables or parameters were exported in this bundle.

## 10. Developer Notes
* **UNCERTAIN: Object Context:** Only a single `JOBI` (Job Include) object was provided. Its exact execution context and the jobs that include it are unknown from this extraction.
* **# REVIEW-STRUCT: Missing Source Code:** The actual script body of `DW.DWH_ADM_JOB_MONITOR_START` is not present in this metadata export. The developer must manually extract the raw UC4 script of this JOBI to identify the exact auditing/monitoring registration logic (e.g., database insert statements, logging formats, API calls, or file creation commands).
* **# REVIEW-STRUCT: Migration Strategy:** JOBI objects must not be migrated as standalone DAGs. Instead, migrate this utility into a shared Python module (e.g., `plugins/utils/job_monitor.py`) containing a reusable helper function. This function can then be invoked by tasks across all migrated workflows to maintain uniform audit/monitoring states.

---

## Pseudocode Style

The following outline represents how to implement the shared utility code in a pythonic, reusable pattern for Airflow, rather than as a standalone workflow.

```python
# ==============================================================================
# ── Imports ───────────────────────────────────────────────────────────────────
# ==============================================================================
import logging
from datetime import datetime
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# ==============================================================================
# ── GCP / Environment Configuration ───────────────────────────────────────────
# ==============================================================================
# # REVIEW-STRUCT: Define the target system (e.g., BigQuery, Cloud SQL, or API endpoint)
# where the monitoring/audit logs are written.
AUDIT_CONN_ID = "gcp_audit_metadata_db"
AUDIT_TABLE = "your_project.monitoring_dataset.job_execution_log"

# ==============================================================================
# ── Shared JOBI Utility Code (DW.DWH_ADM_JOB_MONITOR_START) ───────────────────
# ==============================================================================
# UNCERTAIN: The original UC4 JOBI script logic is missing. This is a standard
# implementation blueprint to register a task or workflow run initialization.

def execute_job_monitor_start(context: dict, **kwargs) -> str:
    """
    Python utility function representing the migrated DW.DWH_ADM_JOB_MONITOR_START JOBI.
    This should be called at the beginning of DAGs or via task pre_execute hooks.
    """
    dag_run = context.get('dag_run')
    task_instance = context.get('ti')
    
    dag_id = dag_run.dag_id if dag_run else "unknown_dag"
    task_id = task_instance.task_id if task_instance else "unknown_task"
    run_id = dag_run.run_id if dag_run else "manual__" + datetime.utcnow().isoformat()
    start_time = datetime.utcnow()

    logging.info(f"Executing DW.DWH_ADM_JOB_MONITOR_START for {dag_id}.{task_id} (Run: {run_id})")

    # # REVIEW-STRUCT: Implement the exact metadata recording mechanism here once
    # the original UC4 JOBI script body is available.
    #
    # Example SQL Execution:
    # insert_sql = f"""
    #     INSERT INTO `{AUDIT_TABLE}` (dag_id, task_id, run_id, status, start_time)
    #     VALUES ('{dag_id}', '{task_id}', '{run_id}', 'RUNNING', '{start_time}')
    # """
    #
    # Code should handle writing to the monitoring DB or publishing to Pub/Sub.
    
    return run_id

# ==============================================================================
# ── Custom Base Operator Mixin Example ────────────────────────────────────────
# ==============================================================================
# Developers can use this custom hook to ensure all migrated tasks execute the
# monitoring start logic automatically.

class MonitoredOperatorMixin:
    """
    A mixin to automatically trigger DW.DWH_ADM_JOB_MONITOR_START 
    pre-execution.
    """
    def pre_execute(self, context):
        super().pre_execute(context)
        execute_job_monitor_start(context)
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `uc4_airflow/DW.DWH_ADM_JOB_MONITOR_START.xml` | `uc4_airflow/dw_dwh_adm_job_monitor_start.py` | Converts the UC4 Job Include (JOBI) startup logic into a reusable Python utility function/module that can be imported by Airflow DAGs to register active runs. |

# Schedule & Variables — Must Be Retained

The following legacy variables must be dynamically populated and processed at runtime by the migrated Python utility block within the Airflow environment:

*   **ADMJP** = `context['dag'].dag_id`: Sourced dynamically from the Airflow execution context to represent the parent Job Plan (DAG) name.
*   **ADMJOB** = `context['task'].task_id` (or `context['dag'].dag_id` if monitoring DAG executions): Sourced dynamically to represent the running job name.
*   **ADMNRJOB** = `context['run_id']`: Sourced dynamically from the execution context to represent the unique execution run number/ID.
*   **DWH_JOB_KENNUNG** = `""`: Initialized as an empty string. Can be set or passed as a default parameter in the Python utility function.
*   **ADMMONJP** = `PREP_PROCESS_VAR("DW.DWH_MONITORED_JPS")`: Mapped to a query against the BigQuery metadata table `dwh_monitored_jps` or retrieved from Airflow Variable `DW_DWH_MONITORED_JPS`.
*   **ADMGB** = `row['dag_id']` / Column 1 from `DW.DWH_MONITORED_JPS`: Mapped to the DAG ID field of the query results.
*   **ADMWERT** = `row['monitoring_enabled_flag']` / Column 2 from `DW.DWH_MONITORED_JPS`: Mapped to the active/enabled flag (where value is `"J"` / `"Y"`).
*   **DW.DWH_RUNNING_JOBS** update (key = `&ADMJOB`, value = `&ADMNRJOB`): Mapped to a BigQuery table insert/upsert or Airflow state write into `dwh_running_jobs` to register the active run.

# External System Replacements

*   **UC4 VARA Object `DW.DWH_MONITORED_JPS`**: Replaced with BigQuery table `dwh_monitored_jps` (or an Airflow Variable containing JSON configuration). BigQuery is preferred for persistent enterprise metadata management.
*   **UC4 VARA Object `DW.DWH_RUNNING_JOBS`**: Replaced with BigQuery table `dwh_running_jobs` (managed as an append-only transaction log or updated via BigQuery DML merge).

# Cross-File Dependencies

*   **Shared Utility Module**: Since `DW.DWH_ADM_JOB_MONITOR_START` is a UC4 Job Include (`JOBI`), it has no independent execution. In Apache Airflow, it must be imported and invoked by all monitored DAGs. To preserve the "include" behavior, it should be registered as a custom DAG policy, an `on_execute_callback` hook, or imported as a helper function called by the first task in each DAG.

# Target File Plan

| Target File Path | Language | Source File | Purpose |
| :--- | :--- | :--- | :--- |
| `uc4_airflow/dw_dwh_adm_job_monitor_start.py` | Python | `uc4_airflow/DW.DWH_ADM_JOB_MONITOR_START.xml` | Shared Python library containing the start execution monitor hook and the BigQuery log writing logic. |

# Environment-Specific Values

*   **GLOBAL**:
    *   `GCP_PROJECT`: The target GCP Project ID. Sourced at runtime via `Variable.get("GCP_PROJECT")` or `os.environ.get("GCP_PROJECT")`.
    *   `BQ_DATASET`: The shared administrative/monitoring BigQuery dataset name. Sourced at runtime via `Variable.get("BQ_DATASET")`.
    *   `BQ_CONNECTION_ID`: The Airflow connection ID used to interact with BigQuery (e.g., `google_cloud_default`).
*   **JOB-SPECIFIC**:
    *   `MONITORED_JPS_TABLE`: The specific table name representing `DW.DWH_MONITORED_JPS`. Sourced as `f"{GCP_PROJECT}.{BQ_DATASET}.dwh_monitored_jps"`.
    *   `RUNNING_JOBS_TABLE`: The specific table name representing `DW.DWH_RUNNING_JOBS`. Sourced as `f"{GCP_PROJECT}.{BQ_DATASET}.dwh_running_jobs"`.

# Risks and Manual Steps

*   **Correction of MCP Tool Code Extraction Error**: The automated MCP tool output stated that the source script was missing. The script is present in the source XML under the `<MSCRI>` block and contains the registration checks. Developers must use the logic defined in this design document (and verified in the source XML) rather than following the MCP's "missing code" placeholder recommendations.
*   **BigQuery Concurrency Limits**: Direct simultaneous updates/upserts from multiple concurrently starting DAG runs on a single BigQuery table (`dwh_running_jobs`) can lead to rate limit errors or DML transaction conflicts. To mitigate this risk, implement `dwh_running_jobs` as an **append-only** log table, and use a BigQuery view with `QUALIFY ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY execution_date DESC) = 1` to read the active state.
*   **Output/Print Literal Compliance**: Any logging or printing carried over must retain original text and language character-for-character. If printing or logging, developers must use:
    *   `print("Added &ADMJOB with &ADMNRJOB")` -> Translated to: `logging.info(f"Added {admjob} with {admnrjob}")`
    *   If the commented-out logging is restored: `PRINT "Job &ADMJOB mit RNR &ADMNRJOB gestartet aus &ADMJP"` -> Translated to: `logging.info(f"Job {admjob} mit RNR {admnrjob} gestartet aus {admjp}")`