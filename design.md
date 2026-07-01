# MIGRATION DESIGN DOCUMENT: DW.BERT_AUSD_BP_TA_BCP_MSISDN

This document details the migration plan and design for converting the UC4 UNIX Job `DW.BERT_AUSD_BP_TA_BCP_MSISDN` to Google Cloud Platform (GCP) with Apache Airflow (Cloud Composer) as the orchestrator and BigQuery as the target data warehouse platform.

---

## VERBATIM MCP TOOL OUTPUT

Below is the complete output returned by the `uc4_to_airflow_dag_design` tool. This must be used by the Build Agent verbatim.

=== Result for vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml ===
## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
This UC4 export contains a single Unix job: `DW.BERT_AUSD_BP_TA_BCP_MSISDN`. It appears to run a shell script that prepares “instantiierten Basisprodukte” and then reads logs afterward. No JOBP, JSCH, or EVNT_TIME objects were provided, so this is not a complete workflow definition by itself. The job is configured with an estimated runtime of 6 seconds and is active at export time.

### 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Description |
|---|---|---:|---|
| `DW.BERT_AUSD_BP_TA_BCP_MSISDN` | `JOBS_UNIX` | `1` | Unix shell job for `BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte` |

### 3. Airflow DAG Properties

| Property | Value |
|---|---|
| dag_id | `dw_bert_ausd_bp_ta_bcp_msisdn` |
| schedule | Not available — no `EVNT_TIME` file provided |
| start_date | `{{ placeholder_start_date }}` |
| catchup | `False` |
| max_active_runs | `1` |
| is_paused_upon_creation | `False` |
| default_args.owner | `uc4_migration` |
| default_args.retries | `0` unless overridden by task-level mapping |
| default_args.retry_delay | `timedelta(seconds=0)` unless overridden by task-level mapping |

Notes:
- No scheduling object was provided, so no cron expression can be derived.
- The UC4 object is active, so no pause-on-create handling is required.

### 4. Task Inventory

This export contains one executable UC4 job and no workflow container. In Airflow, this would typically become one task in a DAG, unless later wrapped by a parent JOBP/JSCH.

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---:|---|---|---|---|---|---|
| `bert_ausd_bp_ta_bcp_msisdn` | `DataprocSubmitJobOperator` | `r_ausd_bp_ta_bcp_msisdn.py` | `project_id=YOUR_GCP_PROJECT_ID; region=YOUR_DATAPROC_REGION; cluster_name=YOUR_DATAPROC_CLUSTER_NAME; bucket=YOUR_BUCKET_NAME; main_python_file_uri=gs://YOUR_BUCKET_NAME/pyspark_scripts/r_ausd_bp_ta_bcp_msisdn.py` | `0` | `none` | None | None | None | None | UC4 script is a shell wrapper that invokes `r_ausd_bp_ta_bcp_msisdn.ksh`; no explicit UC4 retry/postcondition logic was present |

### 5. Task Dependency Map

Because only one JOBS_UNIX object was provided and no JOBP/JSCH container exists in the input, the dependency chain is a single task:

`start >> bert_ausd_bp_ta_bcp_msisdn >> end`

Plain English:
- The DAG starts.
- The Dataproc task runs the PySpark equivalent of the Ab Initio graph inferred from the shell script.
- The DAG ends.

No sensors, calendar checks, or guard tasks are required from the provided XML.

### 6. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| UC4 object name | `DW.BERT_AUSD_BP_TA_BCP_MSISDN` | `dag_id = dw_bert_ausd_bp_ta_bcp_msisdn` |
| UC4 login | `DW.UNIX.ISBERT` | Documented in task metadata; no direct Airflow equivalent unless used for connection mapping |
| UC4 host | `|DWHDWH2P|HOST` | Airflow/Dataproc runtime target; likely represented by Dataproc cluster selection |
| UC4 estimated runtime | `Ert=6` | Informational only; no direct Airflow parameter |
| UC4 script path | `&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh` | Inferred PySpark script: `r_ausd_bp_ta_bcp_msisdn.py` |
| GCP project ID | not present | `YOUR_GCP_PROJECT_ID` |
| Dataproc region | not present | `YOUR_DATAPROC_REGION` |
| Dataproc cluster name | not present | `YOUR_DATAPROC_CLUSTER_NAME` |
| GCS bucket | not present | `YOUR_BUCKET_NAME` |
| PySpark script URI | derived | `gs://YOUR_BUCKET_NAME/pyspark_scripts/r_ausd_bp_ta_bcp_msisdn.py` |

Sanitised Airflow DAG ID mapping:
- `DW.BERT_AUSD_BP_TA_BCP_MSISDN` → `dw_bert_ausd_bp_ta_bcp_msisdn`

### 7. Error Handling and Retry Strategy

#### Task: `bert_ausd_bp_ta_bcp_msisdn`
- UC4 `RUNTIME` section shows `Ert=6`, but no explicit UC4 restart/retry policy is defined.
- No postconditions, restart counts, or block actions were present.
- Airflow mapping:
  - `retries=0`
  - `retry_delay` not applicable
  - `on_failure_callback=None`
- No `ENDED_SKIPPED` handling applies.
- No sync object behavior applies.

#### Sync Object Analysis
- `<SYNCREF>` contains no sync rows.
- No `Else=Wait` or `Else=Skip` behavior to map.

### 8. Developer Notes

- No `EVNT_TIME` file was provided, so the DAG schedule cannot be derived.
- No `JOBP` or `JSCH` file was provided, so there is no workflow container or dependency chain beyond a single task.
- The UC4 job script is a shell script wrapper, not a directly visible Ab Initio graph invocation. The PySpark script name was inferred from the shell script name only.
- GCP placeholders must be filled in manually:
  - `YOUR_GCP_PROJECT_ID`
  - `YOUR_DATAPROC_REGION`
  - `YOUR_DATAPROC_CLUSTER_NAME`
  - `YOUR_BUCKET_NAME`
- No UC4 retry/postcondition logic was present, so Airflow retries are left at default `0`.
- No calendar constraints, earliest-start constraints, or sync constraints were present.
- The source UC4 object was active at export time, so `is_paused_upon_creation=False`.

---

## SECTION 2 — PSEUDOCODE

### Imports
- import `datetime.timedelta`
- import Airflow DAG
- import `DataprocSubmitJobOperator`
- import any required Airflow connection/config helpers if needed
- no `AirflowSkipException` needed because no Else=Skip guard task exists

### GCP Configuration
- define `GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"`
- define `DATAPROC_REGION = "YOUR_DATAPROC_REGION"`
- define `DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"`
- define `GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"`
- define `PYSPARK_SCRIPT_URI = "gs://YOUR_BUCKET_NAME/pyspark_scripts/r_ausd_bp_ta_bcp_msisdn.py"`

### Default Args
- define `default_args`:
  - `owner = "uc4_migration"`
  - `depends_on_past = False`
  - `retries = 0`
  - `retry_delay = timedelta(seconds=0)`
  - `start_date = <placeholder_start_date>`

### on_failure_callback stubs
- none required, because no UC4 failure handling was present

### DAG Definition
- define DAG:
  - `dag_id = "dw_bert_ausd_bp_ta_bcp_msisdn"`
  - `schedule = None`
  - `catchup = False`
  - `max_active_runs = 1`
  - `is_paused_upon_creation = False`
  - `default_args = default_args`

### Task: `bert_ausd_bp_ta_bcp_msisdn`
- create `DataprocSubmitJobOperator`
- parameters:
  - `task_id = "bert_ausd_bp_ta_bcp_msisdn"`
  - `project_id = GCP_PROJECT_ID`
  - `region = DATAPROC_REGION`
  - `job =`
    - `placement.cluster_name = DATAPROC_CLUSTER_NAME`
    - `pyspark_job.main_python_file_uri = PYSPARK_SCRIPT_URI`
    - include any standard Dataproc job fields required by implementation
  - `job_id = dag.dag_id + "_" + run_id + "_bert_ausd_bp_ta_bcp_msisdn"` or equivalent dynamic unique ID
  - `retries = 0`
  - `retry_delay = timedelta(seconds=0)`
  - `on_failure_callback = None`
- note:
  - do not set `TriggerRule.ALL_DONE`
  - leave default trigger rule as `ALL_SUCCESS`

### Dependencies
- `start >> bert_ausd_bp_ta_bcp_msisdn >> end`

---

## EXTRA MIGRATION CONTEXT & REPLACEMENT STRATEGY

### 1. Lineage and Call Hierarchy
*   **Upstream Include Elements**:
    *   `DW.HOLE_PFAD`: A UC4 script block executed first (`:inc DW.HOLE_PFAD`). This on-premise logic dynamically establishes operational paths and directories. For GCP, this is replaced by Airflow environment configuration variables or dynamic GCS folder paths.
    *   `. $HOME/.dw_init`: On-premise UNIX profile initialization. This maps to GCP environment variables injected directly into the execution container or Airflow environment parameters.
*   **Target Core Execution**:
    *   Executes script: `r_ausd_bp_ta_bcp_msisdn.ksh`
    *   Purpose: Prepares "instantiierten Basisprodukte" in the `BERT_P_BASISPRODUKT` model suite.
*   **Downstream Include Elements**:
    *   `DW.BERT_LESE_LOG`: Processed post-execution (`:inc DW.BERT_LESE_LOG`) to read execution and database logs. In Airflow, this is handled natively via GCP Cloud Logging or Airflow's built-in log aggregation.

### 2. External System Replacements & BigQuery Mapping
*   **Host Migration**:
    *   The legacy host `|DWHDWH2P|HOST` and UNIX profile login `DW.UNIX.ISBERT` are replaced by a dedicated Google Cloud Service Account (e.g., `dwh-bert-sa@<project-id>.iam.gserviceaccount.com`) configured with IAM permissions for BigQuery and GCS.
*   **Data Processing Environment**:
    *   Although the generic tool recommended a Dataproc/PySpark pipeline (`r_ausd_bp_ta_bcp_msisdn.py`), since the target platform is **BigQuery**, the underlying SQL queries executed within `r_ausd_bp_ta_bcp_msisdn.ksh` must be migrated to direct **BigQuery SQL**.
    *   The execution mechanism will be the Airflow `BigQueryInsertJobOperator` executing standard SQL residing in the BigQuery target dataset, ensuring optimal native scaling and performance.

### 3. Cross-File Dependencies & Shared Configurations
*   **Shared Environment Properties**:
    *   `DW.HOLE_PFAD` variables are mapped directly into Airflow as variables (`var.value.get('dwh_home_path')`) or a shared Python configuration module in the DAG directory.
    *   Any staging tables used by `r_ausd_bp_ta_bcp_msisdn.ksh` during basic product preparation must reside in a shared BigQuery dataset (e.g., `dw_bert_staging`).

### 4. Target File Plan

| Source File | Target File Path | Language | Purpose / Operator |
|---|---|---|---|
| `DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml` | `dags/dw_bert_ausd_bp_ta_bcp_msisdn.py` | Python (Airflow DAG) | Orchestrator definition |
| `r_ausd_bp_ta_bcp_msisdn.ksh` | `dags/sql/dw_bert_ausd_bp_ta_bcp_msisdn.sql` | SQL (BigQuery Dialect) | Target logic executing database operations via `BigQueryInsertJobOperator` |

### 5. Environment-Specific Variables
These placeholders must be populated during deployment using the project CICD variables or Airflow Connection configurations:
*   `GCP_PROJECT_ID`: Target GCP Project ID (e.g., `gcp-dwh-prod`)
*   `BQ_DATASET_BERT`: Target dataset name (e.g., `dw_bert`)
*   `GCS_BUCKET_NAME`: Storage bucket for queries and temp results (e.g., `gs://dw-bert-artifacts/`)
*   `CONN_ID_BIGQUERY`: Connection name in Airflow (e.g., `bigquery_default`)

### 6. Risks, Mitigation, and Manual Steps
*   **Unresolved Script Details**: The inside of the shell script `r_ausd_bp_ta_bcp_msisdn.ksh` is not present. A database developer must manually extract the core SQL execution from that `.ksh` script and format it as a clean Standard SQL file for `dags/sql/dw_bert_ausd_bp_ta_bcp_msisdn.sql`.
*   **Restart Behavior**: The documentation block states "Restart jederzeit möglich" (Restart possible at any time). To preserve this idempotent behavior in BigQuery, the SQL queries must use write disposition `WRITE_TRUNCATE` or appropriate `MERGE` commands to overwrite previous run executions cleanly.