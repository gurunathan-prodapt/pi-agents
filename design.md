# MIGRATION DESIGN DOCUMENT: BERT_DROP_TEMP_TABLE

This document outlines the migration design for the UC4 UNIX Job `DW.BERT_DROP_TEMP_TABLE` to Google Cloud Platform (GCP) utilizing Apache Airflow (Cloud Composer) and Google BigQuery. 

---

## SECTION 1 — VERBATIM MCP TOOL OUTPUT

Below is the verbatim output returned by the UC4 conversion tool:

```markdown
=== Result for vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_DROP_TEMP_TABLE.xml ===
## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
This UC4 object is a single Unix job named `DW.BERT_DROP_TEMP_TABLE`. It appears to run a shell script that drops temporary tables, likely as part of a BERT-related data maintenance workflow. The job runs on a UNIX host using login `DW.UNIX.ISBERT` and has an estimated runtime of 7 seconds. No upstream/downstream JOBP or JSCH workflow container was provided, so this is only the job-level conversion blueprint, not a complete multi-step workflow.

### 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Description |
|---|---|---:|---|
| `DW.BERT_DROP_TEMP_TABLE` | `JOBS_UNIX` | `1` | Unix job that executes `r_drop_temp_table.ksh` after sourcing environment setup and includes log-reading include statements. |

### 3. Airflow DAG Properties

| Property | Value |
|---|---|
| dag_id | `dw_bert_drop_temp_table` |
| schedule | Not derivable from provided input; no EVNT_TIME file present |
| start_date | `placeholder_start_date` |
| catchup | `False` |
| max_active_runs | `1` |
| is_paused_upon_creation | `False` |
| default_args.owner | `uc4_migration` |
| default_args.retries | `0` unless overridden by task-level UC4 retry logic |
| default_args.retry_delay | `timedelta(seconds=0)` unless overridden |
| default_args.start_date | `placeholder_start_date` |

Notes:
- No `EVNT_TIME` file was provided, so no cron schedule can be derived.
- The source object is active, so no pause-on-create handling is needed.

### 4. Task Inventory

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---:|---|---|---|---|---|---|
| `run_bert_drop_temp_table` | `DataprocSubmitJobOperator` | `r_drop_temp_table.py` | `project_id=YOUR_GCP_PROJECT_ID`, `region=YOUR_DATAPROC_REGION`, `cluster_name=YOUR_DATAPROC_CLUSTER_NAME`, `bucket=YOUR_BUCKET_NAME`, `main_python_file_uri=gs://YOUR_BUCKET_NAME/pyspark_scripts/r_drop_temp_table.py` | `0` | `0` | None | None | N/A | None | Derived from UC4 script command `r_drop_temp_table.ksh`; no `-j/-k/-t` parameters were present, so the exact Ab Initio graph/job metadata is not available. |

Important mapping note:
- The UC4 script body does not contain an `r_ai_start` command, so there is no Ab Initio graph name, job key, or job type to extract.
- The job is a standalone Unix script job, so the Airflow implementation should submit the equivalent PySpark job directly rather than using `TriggerDagRunOperator`.

### 5. Task Dependency Map

Linear chain:

`start >> run_bert_drop_temp_table >> end`

Plain-English flow:
- The DAG starts.
- The Dataproc job task runs the converted PySpark equivalent of `r_drop_temp_table.ksh`.
- The DAG ends after the job completes successfully.

No sensor tasks, calendar checks, or guard tasks are required from the provided input.

### 6. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `DW.BERT_DROP_TEMP_TABLE` | UC4 object name | `dag_id = dw_bert_drop_temp_table` |
| `Active` | `<Active>1</Active>` | Deploy normally; no pause flag |
| `Login` | `DW.UNIX.ISBERT` | Airflow connection / runtime identity placeholder for Dataproc execution context |
| `HostDst` | `|DWHDWH2P|HOST` | Dataproc cluster target placeholder; manual mapping required |
| `Ert` | `7` | Informational runtime estimate; may be used for monitoring only |
| Script path | `&HOME/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh` | PySpark script placeholder: `gs://YOUR_BUCKET_NAME/pyspark_scripts/r_drop_temp_table.py` |

Sanitised Airflow DAG ID mapping:
- No `trigger_dag_id` is required because this job is not a workflow trigger task.

### 7. Error Handling and Retry Strategy

- UC4 runtime estimate: 7 seconds.
- No explicit UC4 restart/retry postcondition was provided in the XML.
- No `RESTART TASK`, `BLOCK`, or `EXECUTE OBJECT` failure branch was present.
- Therefore, the Airflow task should use the default failure behavior with no special `on_failure_callback`, unless the Build stage introduces a standard platform-wide alerting callback.

Task-specific mapping:
- `run_bert_drop_temp_table`
  - Retries: `0`
  - Retry delay: none
  - on_failure_callback: none required from source
  - ENDED_SKIPPED handling: not applicable
  - Sync Else behavior: all sync rows are `Else="Wait"`, which would map to `max_active_runs=1` if this were a DAG-level sync pattern; however, no DAG-level sync guard is needed from the provided object alone.

### 8. Developer Notes

- No `EVNT_TIME` file was provided, so the schedule cannot be converted to cron.
- No `JOBP` or `JSCH` workflow container was provided, so there is no task dependency chain beyond this single job.
- The UC4 script body does not contain `r_ai_start -j/-k/-t`, so there is no Ab Initio graph metadata to map.
- The PySpark script name `r_drop_temp_table.py` is an inferred equivalent based on the shell script name; this should be validated by the migration team.
- GCP placeholders must be replaced manually:
  - `YOUR_GCP_PROJECT_ID`
  - `YOUR_DATAPROC_REGION`
  - `YOUR_DATAPROC_CLUSTER_NAME`
  - `YOUR_BUCKET_NAME`
- No calendar constraints were present.
- No earliest-start constraint was present.
- No UC4 retry/restart semantics were present in the XML.
- Source object was active at export time, so no `is_paused_upon_creation=True` is needed.

---

## SECTION 2 — PSEUDOCODE

### Imports
- `from datetime import timedelta`
- `from airflow import DAG`
- `from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator`

### GCP Configuration
- `GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"`
- `DATAPROC_REGION = "YOUR_DATAPROC_REGION"`
- `DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"`
- `GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"`

### Default Args
- `default_args = {`
  - `owner: "uc4_migration"`
  - `depends_on_past: False`
  - `retries: 0`
  - `retry_delay: timedelta(seconds=0)`
  - `start_date: placeholder_start_date`
- `}`

### on_failure_callback stubs
- None required from the provided UC4 source.

### DAG Definition
- `with DAG(`
  - `dag_id="dw_bert_drop_temp_table"`
  - `schedule=None`  # no EVNT_TIME provided
  - `catchup=False`
  - `max_active_runs=1`
  - `is_paused_upon_creation=False`
  - `default_args=default_args`
- `) as dag:`

### Task: `run_bert_drop_temp_table`
- Operator: `DataprocSubmitJobOperator`
- Parameters:
  - `task_id="run_bert_drop_temp_table"`
  - `project_id=GCP_PROJECT_ID`
  - `region=DATAPROC_REGION`
  - `job={`
    - `reference: { job_id: dag.dag_id + "_" + run_id + "_run_bert_drop_temp_table" }`
    - `placement: { cluster_name: DATAPROC_CLUSTER_NAME }`
    - `pyspark_job: {`
      - `main_python_file_uri: "gs://" + GCS_BUCKET_NAME + "/pyspark_scripts/r_drop_temp_table.py"`
    - `}`
  - `}`
- Retry configuration:
  - `retries=0`
  - `retry_delay=timedelta(seconds=0)`
- `on_failure_callback=None`
- Notes:
  - This is the PySpark equivalent of the UC4 Unix script job.
  - No `TriggerDagRunOperator` is needed.

### Dependencies
- `start >> run_bert_drop_temp_table >> end`
```

---

## SECTION 3 — COMPLEMENTARY MIGRATION CONTEXT

This section contains operational, architectural, and target-system details that were not accessible to the automated MCP parser.

### 1. Detailed Lineage and Coordination
* **Upstream Sync Resources**: The UC4 job utilizes synchronizations (`SYNCREF`) against multiple active lock objects:
  * `DW.BERT_RECH_SYNC`
  * `DW.BERT_VERT_SYNC`
  * `DW.BERT_GP_SYNC`
  * `DW.BERT_BASIS_SYNC`
  * `DW.BERT_ADRESS_SYNC`
  * `DW.BERT_STAMM_SYNC`
  
  In the legacy system, these sync parameters prevent the table-dropping routine from running concurrently with jobs processing Account, Contract, Partner, Foundation, Address, and Master data.
* **Airflow Mapping**: In Google Cloud Composer, these locks should be implemented using **Airflow Pools** with a slot capacity of `1` (e.g., `pool="bert_write_lock_pool"`). Alternatively, if this job is integrated into a master schedule (e.g. `DW.BERT_STAMMDATEN_JP` DAG), it must run sequentially as the terminal task once all loading tasks for these domains complete successfully.
* **Legacy Includes**: 
  * `:inc DW.HOLE_PFAD` maps to Airflow Environment Variables (`var.value.dwh_base_gcs_path`) rather than local file system lookups.
  * `:inc DW.BERT_LESE_LOG` maps to native Airflow job logs and standard task failure handlers.

### 2. External System Replacements
* **Host and Login Mapping**:
  * Legacy host `|DWHDWH2P|HOST` and UNIX login `DW.UNIX.ISBERT` map to a Google Service Account (`sa-composer-bert@<project-id>.iam.gserviceaccount.com`) configured in Airflow with IAM access to BigQuery.
* **Alternative Cost-Effective Design (Standard SQL vs. Dataproc PySpark)**:
  * Because the original job `r_drop_temp_table.ksh` runs a SQL utility to drop temporary schema objects, deploying a Dataproc PySpark cluster simply to issue `DROP TABLE` statements is highly inefficient and cost-prohibitive.
  * **Optimized Pattern**: Convert the shell utility into standard **BigQuery DDL SQL statements** executed via the `BigQueryInsertJobOperator`. This drops table objects instantly at minimal cost.

### 3. Cross-File Dependencies & Shared Resources
* **Target Schema**: Shared temporary tables typically reside in a scratch dataset (e.g., `dwh_bert_staging`). 
* **Call Chain**:
  `DW.BERT_STAMMDATEN_JP` (Workflow) -> Calls `DW.BERT_DROP_TEMP_TABLE` -> Invokes SQL script `r_drop_temp_table.sql`.

### 4. Target File Plan

| Target File Path | Target Language | Source Legacy Component | Description |
|---|---|---|---|
| `dags/dw_bert_drop_temp_table.py` | Python (Airflow DAG) | `DW.BERT_DROP_TEMP_TABLE.xml` | Airflow DAG orchestrating the drop job. |
| `sql/bert/r_drop_temp_table.sql` | BigQuery Standard SQL | `r_drop_temp_table.ksh` | Contains the BigQuery SQL DDL query (`DROP TABLE IF EXISTS...`) to clean up staging tables. |

### 5. Environment-Specific Values

The configuration parameters for Composer/Airflow execution should utilize Airflow Variables to allow seamless transitions across environments:

| Parameter Key | Dev Value | Prod Value | GCS/Airflow Mapping |
|---|---|---|---|
| `gcp_project_id` | `gcp-dev-dwh-1` | `gcp-prod-dwh-1` | `{{ var.value.gcp_project_id }}` |
| `gcs_bucket` | `gs://dev-dwh-bert-assets` | `gs://prod-dwh-bert-assets` | `{{ var.value.gcs_bucket }}` |
| `staging_dataset` | `dev_bert_staging` | `prod_bert_staging` | `{{ var.value.bert_staging_dataset }}` |
| `gcp_conn_id` | `google_cloud_default` | `google_cloud_default` | BigQuery Operator Connection ID |

### 6. Risks and Manual Steps
1. **Hidden SQL Logic**: The legacy script `r_drop_temp_table.ksh` contains the specific table names being dropped. Developers must inspect this legacy script on-disk to identify the exact list of tables (e.g., `temp_rech`, `temp_vert`, etc.) and populate the corresponding `sql/bert/r_drop_temp_table.sql` file.
2. **Synchronization Constraints**: Ensure that the target Airflow Pool (`bert_write_lock_pool`) or task dependencies are properly defined across all migrated BERT pipelines to prevent active loading jobs from failing due to tables being dropped mid-run.