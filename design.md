# MIGRATION DESIGN DOCUMENT — JOB: EXIS

---

## EXECUTIVE SUMMARY & MIGRATION ARCHITECTURE

This document provides an implementation-ready design for migrating the legacy data exporter job **EXIS** to Google Cloud Platform (GCP). 

### 1. Source Environment Overview
The legacy **EXIS** job consists of four primary export tasks that extract master data, option associations, GPRS data, voice connections, and contract discounts from an Oracle Data Warehouse.
- **Orchestration**: Managed by UC4 (Automic) scheduler via Unix jobs (`JOBS_UNIX`).
- **Exporter Execution Framework**: A heavy, custom, 83,000-line KornShell framework script (`r_exis_v2`) that parses configuration parameters (`.var` files), runs parallel database extracts via `SQL*Plus`, reformats streams using `nawk`, compresses outputs with `gzip`, and transfers files to target servers via SFTP.
- **Data Queries**: Composed of native Oracle SQL queries using Oracle parallel hints, table joins, and string aggregation functions (`LISTAGG`).

### 2. Target GCP Cloud-Native Architecture
To modernize this job, we will completely retire the KSH framework (`r_exis_v2`) and replace it with a serverless, robust GCP-native pipeline.
- **Orchestration**: Cloud Composer (Apache Airflow 2) DAGs running native GCP operators.
- **Data Processing & Spooling**: Google BigQuery handles the high-performance SQL extraction, writing query results directly to Google Cloud Storage (GCS) using `EXPORT DATA`.
- **Formatting, Compression & Custom Trailer Generation**: A lightweight, containerized Python task (via `PythonOperator` or `KubernetesPodOperator` in Cloud Composer) that streams the GCS-extracted CSV file, calculates the row count (`NR`), appends the custom trailer record matching the legacy `nawk` logic, and compresses the final output to a `.csv.gz` file.
- **Secure File Transfer**: Standard Airflow `SFTPOperator` utilizing secret credentials fetched securely from Google Secret Manager.

---

# SECTION 1 — VERBATIM MCP TOOL OUTPUTS

Below is the complete, unmodified output returned by the UC4-to-Airflow conversion tool for each of the four UC4 jobs making up the EXIS pipeline.

=== Result for vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_DATA.xml ===
## Input validation result

I found **one UC4 XML file** in the input:

- `JOBS_UNIX` object: `DW.DWH_EXIS_SD_APT_NNA_DATA`

Because only **one file** was provided and it is **not** an `EVNT_TIME` file, this is **not a complete workflow export**. A complete UC4 workflow typically also needs at least:
- one `EVNT_TIME`
- one `JOBP`
- one `JOBS_UNIX`

I can still produce a partial design document for this single job, but note that **schedule, workflow dependencies, and trigger DAG mappings cannot be fully determined** from this input alone.

---

# SECTION 1 — DESIGN DOCUMENT

## 1. Overview

This UC4 job is a Unix job that exports telephone system master data. The script sets a job identifier, loads a shell initialization file, derives a month-based date value from the current system date, and then runs an export command (`r_exis_v2`) with a configuration file and the derived month parameter. The documentation indicates that the output is a compressed CSV file named like `DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz`, which is then distributed to a target system. No upstream/downstream workflow structure is present in the provided file, so this appears to be a standalone job or a fragment of a larger workflow.

## 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Description |
|---|---|---:|---|
| `DW.DWH_EXIS_SD_APT_NNA_DATA` | `JOBS_UNIX` | `1` | Data export job for telephone system master data; runs an export command to generate and distribute a CSV.gz file |

## 3. Airflow DAG Properties

Because no `EVNT_TIME` or `JOBP` file was provided, the DAG schedule and workflow orchestration cannot be fully derived. The table below reflects only what can be safely inferred.

| Property | Value |
|---|---|
| dag_id | `dw_dwh_exis_sd_apt_nna_data` |
| schedule | `None` / manual or externally triggered until an `EVNT_TIME` is provided |
| start_date | `{{ PLACEHOLDER_START_DATE }}` |
| catchup | `False` |
| max_active_runs | `1` |
| is_paused_upon_creation | `False` |
| default_args.owner | `uc4_migration` |
| default_args.retries | `0` unless overridden by UC4 failure semantics |
| default_args.retry_delay | `timedelta(minutes=0)` unless overridden |
| default_args.email / alerts | Not derivable from source; implement only if required by target standards |

### Active flag mapping
- Source UC4 object is **active** (`<Active>1</Active>`), so **no special Airflow pause handling** is required.

## 4. Task Inventory

Only one executable UC4 job is present, so the Airflow design contains one main task. No sensors, calendar checks, or trigger operators can be derived from the provided file.

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---:|---|---|---|---|---|---|
| `dwh_exis_sd_apt_nna_data` | `DataprocSubmitJobOperator` | `r_exis_v2.py` | `project_id=YOUR_GCP_PROJECT_ID`, `region=YOUR_DATAPROC_REGION`, `cluster_name=YOUR_DATAPROC_CLUSTER_NAME`, `main_python_file_uri=gs://YOUR_BUCKET_NAME/pyspark_scripts/r_exis_v2.py` | `0` | `None` | None found | None found | N/A | None required from source | UC4 script calls `r_exis_v2` with `-k` and `-p`; no workflow dependencies available |

### Unix job analysis

#### Extracted from UC4 script body
- **Command invoked:** `r_exis_v2`
- **Configuration / key-like parameter:** `-k $HOME/aktuell/exporter/apt/cfg/h_exis_apt_nna_daten.var`
- **Parameter:** `-p &MONAT_ID`
- **Ab Initio graph name:** **Not explicitly present**
  - The provided script does **not** contain an `r_ai_start` command.
  - Therefore, the Ab Initio graph name, job key, and job type cannot be extracted using the requested `r_ai_start -j/-k/-t` pattern.
- **UC4 login:** `DW.UNIX.ISTNS`
- **Host:** `|DWHDWH1P|HOST`
- **Estimated runtime (ERT):** `10` seconds

### PySpark mapping
Because no Ab Initio graph name was present, there is no direct graph-to-PySpark mapping from the requested rule set. If the Build stage must still convert this job into a Dataproc PySpark submission, the closest safe placeholder is:

- PySpark script: `r_exis_v2.py`
- GCS path: `gs://YOUR_BUCKET_NAME/pyspark_scripts/r_exis_v2.py`

This is an assumption, not a source-derived Ab Initio graph mapping.

## 5. Task Dependency Map

No `JOBP`, `JSCH`, or `EVNT_TIME` objects were provided, so no dependency chain can be derived.

### Minimal chain for the single job
`start >> dwh_exis_sd_apt_nna_data >> end`

### Plain-English execution description
- The DAG starts.
- The single Dataproc submission task runs the export logic represented by the UC4 Unix job.
- The DAG ends after the job completes.

## 6. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `name` | `DW.DWH_EXIS_SD_APT_NNA_DATA` | `dag_id = dw_dwh_exis_sd_apt_nna_data` |
| `Active` | `1` | Deploy normally; do not pause DAG on creation |
| `Login` | `DW.UNIX.ISTNS` | Document as runtime login/credential context; map to Airflow connection or service account as needed |
| `HostDst` | `|DWHDWH1P|HOST` | Document as target host context; likely not directly used in Dataproc submission |
| `Ert` | `10` | `retry_delay`/runtime expectation reference only; not a retry setting |
| `Command` in script | `r_exis_v2` | PySpark script placeholder `r_exis_v2.py` |
| `-k` parameter | `$HOME/aktuell/exporter/apt/cfg/h_exis_apt_nna_daten.var` | Treat as job configuration input; may become a Dataproc job argument or file dependency |
| `-p` parameter | `&MONAT_ID` | Runtime parameter derived from current date in Airflow task logic |
| GCP project | Not present | `YOUR_GCP_PROJECT_ID` |
| Dataproc region | Not present | `YOUR_DATAPROC_REGION` |
| Dataproc cluster | Not present | `YOUR_DATAPROC_CLUSTER_NAME` |
| Bucket | Not present | `YOUR_BUCKET_NAME` |

No UC4 object names are used as `trigger_dag_id` values in this input.

## 7. Error Handling and Retry Strategy

### Source UC4 failure behavior
The documentation states:
- “In case of abort Restart the job without any previous actions (default).”
- “In case of failure The job can be restarted after an interruption with no additional preparation.”

This suggests the job is restartable and does not require special cleanup logic. However, no explicit UC4 restart count or wait interval is defined in the XML.

### Airflow mapping
- **Retries:** `0` by default unless the Build stage or migration standards require a retry policy.
- **Retry delay:** not derivable from source.
- **on_failure_callback:** not required from source.
- **Terminal failure stub:** not required from source.
- **ENDED_SKIPPED:** not applicable; no postcondition tree was provided.
- **Sync Else behavior:** none present.

### Notes
- The UC4 `Ert` value is `10` seconds, but this is an estimated runtime, not a retry policy.
- If the migration team wants to preserve “restartable without preparation” semantics, they may choose to add a small retry policy in Airflow, but that would be an implementation decision rather than a source-derived requirement.

## 8. Developer Notes

- **Missing workflow context:** No `EVNT_TIME`, `JOBP`, or `JSCH` file was provided, so schedule and dependencies are unknown.
- **No Ab Initio graph found:** The script does not contain `r_ai_start -j/-k/-t`; therefore the requested graph/job-type extraction cannot be performed from this source.
- **PySpark mapping is assumed:** `r_exis_v2` was treated as the script name placeholder because no graph name was available.
- **GCP placeholders must be filled manually:**
  - `YOUR_GCP_PROJECT_ID`
  - `YOUR_DATAPROC_REGION`
  - `YOUR_DATAPROC_CLUSTER_NAME`
  - `YOUR_BUCKET_NAME`
- **Cron schedule discrepancy:** Not applicable because no `EVNT_TIME` file was provided.
- **Calendar constraints:** None found.
- **Else=Skip guard task:** None found.
- **ENDED_SKIPPED handling:** Not applicable because no postcondition tree was provided.
- **Retry semantics:** UC4 documentation indicates restartability, but no explicit retry count/wait time is present in XML.
- **Assumption made:** The UC4 Unix job will be migrated as a Dataproc PySpark submission task, even though the source script is a shell command and not an explicit PySpark graph invocation.

---

# SECTION 2 — PSEUDOCODE

## Imports
- `from datetime import timedelta`
- `from airflow import DAG`
- `from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator`
- `from airflow.utils.dates import days_ago` or a placeholder `start_date`
- `from airflow.models import Variable` if runtime configuration is externalized
- `from airflow.exceptions import AirflowSkipException` only if a guard task is later introduced
- `from airflow.operators.python import PythonOperator` only if a guard task is later introduced

## GCP Configuration
- `GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"`
- `DATAPROC_REGION = "YOUR_DATAPROC_REGION"`
- `DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"`
- `GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"`

## Default Args
- `default_args = {`
  - `owner: "uc4_migration"`
  - `depends_on_past: False`
  - `retries: 0`
  - `retry_delay: timedelta(minutes=0)`
  - `start_date: PLACEHOLDER_START_DATE`
- `}`

## on_failure_callback stubs
- None required from source.
- If the Build stage chooses to add a generic failure alarm, define:
  - `on_failure_alarm(context):`
    - `# TODO: implement alerting logic`
- No terminal failure stub is required because no UC4 retry/block semantics were provided.

## DAG Definition
- Define DAG:
  - `dag_id = "dw_dwh_exis_sd_apt_nna_data"`
  - `schedule = None`
  - `catchup = False`
  - `max_active_runs = 1`
  - `is_paused_upon_creation = False`
  - `default_args = default_args`

## Guard Task
- None required because no `Else=Skip` sync was found.

## Sensor Task
- None required because no `ErlstStTime` constraint was found.

## Calendar Check Task
- None required because no calendar constraint was found.

## Task: `dwh_exis_sd_apt_nna_data`
- Operator: `DataprocSubmitJobOperator`
- Task purpose: submit the PySpark job corresponding to the UC4 export logic
- Parameters:
  - `project_id = GCP_PROJECT_ID`
  - `region = DATAPROC_REGION`
  - `cluster_name = DATAPROC_CLUSTER_NAME`
  - `job = {`
    - `reference.job_id = dag.dag_id + "_" + run_id + "_dwh_exis_sd_apt_nna_data"`
    - `placement.cluster_name = DATAPROC_CLUSTER_NAME`
    - `pyspark_job.main_python_file_uri = "gs://YOUR_BUCKET_NAME/pyspark_scripts/r_exis_v2.py"`
    - `pyspark_job.args = ["-k", "$HOME/aktuell/exporter/apt/cfg/h_exis_apt_nna_daten.var", "-p", derived_month_id]`
  - `}`
- Retry configuration:
  - `retries = 0`
  - `retry_delay = timedelta(minutes=0)`
- `on_failure_callback = None` unless a generic alerting policy is added
- Do not set `TriggerRule.ALL_DONE`
- Additional note:
  - `derived_month_id` should replicate UC4 logic:
    - take current execution date in `YYYYMMDD`
    - truncate to `YYYYMM`
  - If the Build stage cannot safely reproduce the shell environment behavior, document this as an implementation assumption

## Dependencies
- `start >> dwh_exis_sd_apt_nna_data >> end`



=== Result for vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_VOIC.xml ===
## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
This UC4 object is a single Unix job that exports telephone system master data. The job appears to generate a CSV/GZIP output file and distribute it to a target system. Based on the provided XML, this is not a complete workflow by itself; it is one executable job with no visible parent JOBP/JSCH orchestration or time-based trigger in the supplied input. The job is active and has an estimated runtime of 10 seconds.

### 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Description |
|---|---|---:|---|
| `DW.DWH_EXIS_SD_APT_NNA_VOIC` | `JOBS_UNIX` | `1` | Data export of telephone system masterdata; exports a CSV/GZIP file and distributes it to the target system. |

### 3. Airflow DAG Properties

| Property | Value |
|---|---|
| dag_id | `dw_dwh_exis_sd_apt_nna_voic` |
| schedule | `None` / manual trigger only |
| start_date | `PLACEHOLDER_START_DATE` |
| catchup | `False` |
| max_active_runs | `1` |
| is_paused_upon_creation | `False` |
| owner | `data_engineering` |
| retries | `0` |
| retry_delay | `timedelta(seconds=0)` |

Notes:
- No `EVNT_TIME` file was provided, so no cron schedule can be derived.
- No `JOBP` or `JSCH` orchestration was provided, so this should be treated as a standalone DAG or a task inside a larger manually created DAG, depending on build-stage design.

### 4. Task Inventory

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---:|---|---|---|---|---|---|
| `run_dw_dwh_exis_sd_apt_nna_voic` | `DataprocSubmitJobOperator` | `r_exis_v2.py` | `project_id=YOUR_GCP_PROJECT_ID; region=YOUR_DATAPROC_REGION; cluster_name=YOUR_DATAPROC_CLUSTER_NAME; bucket=YOUR_BUCKET_NAME; main_python_file_uri=gs://YOUR_BUCKET_NAME/pyspark_scripts/r_exis_v2.py` | `0` | `0s` | None | None | N/A | None | Ab Initio graph inferred from script command `r_exis_v2`; UC4 ERT = 10 seconds. |

Important mapping note:
- The UC4 script body does not contain an `r_ai_start` command. Instead, it invokes `r_exis_v2` directly.
- Per the requested mapping pattern, the equivalent PySpark script name is derived as `r_exis_v2.py` from the executable name found in the script body.

### 5. Task Dependency Map

Since only one executable job is present, the dependency chain is:

`start >> run_dw_dwh_exis_sd_apt_nna_voic >> end`

Plain-English execution:
- The DAG starts.
- The Dataproc job submits the PySpark workload corresponding to the UC4 Unix job.
- The DAG ends after the job completes successfully.

No sensor tasks, calendar checks, or guard tasks are required from the provided XML.

### 6. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `name` | `DW.DWH_EXIS_SD_APT_NNA_VOIC` | `dag_id = dw_dwh_exis_sd_apt_nna_voic` |
| `Active` | `1` | Deploy normally; no pause flag needed |
| `Title` | `Data export of telephone system masterdata.` | DAG/task documentation only |
| `Login` | `DW.UNIX.ISTNS` | Document as source execution login; no direct Airflow equivalent unless using connection metadata |
| `HostDst` | `|DWHDWH1P|HOST` | Document as source host; no direct Airflow equivalent in Dataproc execution |
| `Ert` | `10` | Informational runtime estimate; may be used for monitoring only |
| Script executable | `r_exis_v2` | PySpark script name `r_exis_v2.py` |
| PySpark script URI | derived | `gs://YOUR_BUCKET_NAME/pyspark_scripts/r_exis_v2.py` |
| GCP project | not present | `YOUR_GCP_PROJECT_ID` |
| Dataproc region | not present | `YOUR_DATAPROC_REGION` |
| Dataproc cluster | not present | `YOUR_DATAPROC_CLUSTER_NAME` |
| GCS bucket | not present | `YOUR_BUCKET_NAME` |

### 7. Error Handling and Retry Strategy

- UC4 `RUNTIME/Ert` is `10`, which is an estimate, not a retry policy.
- No UC4 restart/retry postcondition logic is present in the provided XML.
- No `POSTCOND` or equivalent failure branch was provided.
- Therefore:
  - Airflow retries: `0`
  - Airflow retry delay: none
  - `on_failure_callback`: none required from source logic

Sync object behavior:
- No `<SYNCREF>` rows are present.
- No `Else=Wait` or `Else=Skip` mapping is needed.

ENDED_SKIPPED handling:
- Not present in the provided object.
- No special trigger rule changes are required.

### 8. Developer Notes

- No `EVNT_TIME` file was provided, so no cron schedule could be derived.
- No `JOBP` or `JSCH` file was provided, so there is no visible workflow chain beyond this single job.
- The UC4 script does not contain `r_ai_start -j/-k/-t`; instead it calls `r_exis_v2` directly. The Build stage must confirm whether `r_exis_v2` is the intended executable to map to a PySpark script.
- GCP placeholders must be filled in manually:
  - `YOUR_GCP_PROJECT_ID`
  - `YOUR_DATAPROC_REGION`
  - `YOUR_DATAPROC_CLUSTER_NAME`
  - `YOUR_BUCKET_NAME`
- No calendar constraints were present.
- No retry or failure postconditions were present.
- No inactive flag handling is needed because `<Active>1</Active>`.
- Assumption made: the UC4 Unix job is being migrated as a Dataproc-submitted PySpark task, with the executable name converted to a `.py` script name.

---

## SECTION 2 — PSEUDOCODE

── Imports ──────────────────────────────────────────────
- import `timedelta` from `datetime`
- import `DAG` from `airflow`
- import `DataprocSubmitJobOperator` from `airflow.providers.google.cloud.operators.dataproc`

── GCP Configuration ────────────────────────────────────
- `GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"`
- `DATAPROC_REGION = "YOUR_DATAPROC_REGION"`
- `DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"`
- `GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"`
- `PYSPARK_SCRIPT_URI = "gs://YOUR_BUCKET_NAME/pyspark_scripts/r_exis_v2.py"`

── Default Args ─────────────────────────────────────────
- `default_args = {`
  - `owner: "data_engineering"`
  - `retries: 0`
  - `retry_delay: timedelta(seconds=0)`
  - `start_date: PLACEHOLDER_START_DATE`
- `}`

── on_failure_callback stubs ─────────────────────────────
- None required, because no UC4 failure postcondition or restart logic was present.

── DAG Definition ───────────────────────────────────────
- define DAG:
  - `dag_id="dw_dwh_exis_sd_apt_nna_voic"`
  - `schedule=None`
  - `catchup=False`
  - `max_active_runs=1`
  - `is_paused_upon_creation=False`
  - `default_args=default_args`

── Task: run_dw_dwh_exis_sd_apt_nna_voic ────────────────
- operator: `DataprocSubmitJobOperator`
- parameters:
  - `task_id="run_dw_dwh_exis_sd_apt_nna_voic"`
  - `project_id=GCP_PROJECT_ID`
  - `region=DATAPROC_REGION`
  - `job={`
    - `reference: { job_id: dynamic value using dag_id + run_id + task suffix }`
    - `placement: { cluster_name: DATAPROC_CLUSTER_NAME }`
    - `pyspark_job: { main_python_file_uri: PYSPARK_SCRIPT_URI }`
  - `}`
- retries: `0`
- retry_delay: `timedelta(seconds=0)`
- no `on_failure_callback`
- no `wait_for_completion` setting needed
- no `trigger_dag_id`
- note: do not use `TriggerRule.ALL_DONE`

── Dependencies ─────────────────────────────────────────
- `start >> run_dw_dwh_exis_sd_apt_nna_voic >> end`

=== Result for vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_TAEGLICH_JP/DW.DWH_EXIS_SD_APT_BESTANDS.xml ===
## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
This UC4 object is a single Unix job that exports stock data into a compressed CSV file and distributes it to a target system. The job appears to run a shell-based export utility (`r_exis_v2`) using a configuration file for “APT bestandsdaten” (stock data). Based on the provided XML, this is not a full workflow with multiple dependent tasks; it is one standalone job. No explicit schedule object, job plan, or time event was provided, so the run frequency cannot be determined from this input alone.

---

### 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Description |
|---|---|---:|---|
| `DW.DWH_EXIS_SD_APT_BESTANDS` | `JOBS_UNIX` | `1` | Unix job that exports stock data using `r_exis_v2` and a configuration file. |

---

### 3. Airflow DAG Properties

| Property | Value |
|---|---|
| dag_id | `dw_dwh_exis_sd_apt_bestands` |
| schedule | `None` / manual trigger unless a separate EVNT_TIME or JSCH is provided |
| start_date | `{{ placeholder_start_date }}` |
| catchup | `False` |
| max_active_runs | `1` |
| is_paused_upon_creation | `False` |
| default_args.owner | `data-platform` |
| default_args.retries | `0` |
| default_args.retry_delay | `timedelta(minutes=0)` |

Notes:
- No EVNT_TIME file was provided, so no cron schedule can be derived.
- The UC4 object is active, so no pause-on-creation handling is required.

---

### 4. Task Inventory

Since only one JOBS_UNIX object was provided, the Airflow implementation is a single Dataproc PySpark task placeholder mapped from the Unix job’s business function.

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---:|---|---|---|---|---|---|
| `run_dw_dwh_exis_sd_apt_bestands` | `DataprocSubmitJobOperator` | `exis_v2.py` | `project_id=YOUR_GCP_PROJECT_ID`, `region=YOUR_DATAPROC_REGION`, `cluster_name=YOUR_DATAPROC_CLUSTER_NAME`, `main_python_file_uri=gs://YOUR_BUCKET_NAME/pyspark_scripts/exis_v2.py` | `0` | `None` | None | None | N/A | None | Derived from shell command `r_exis_v2`; no `-j` parameter was present, so the graph name is inferred from the executable name only. |

Important caveat:
- The UC4 job is a shell script job, not an explicit PySpark job. The requested migration pattern requires Dataproc/PySpark mapping, but the XML does not contain an Ab Initio graph name (`-j`), job key (`-k`), or job type (`-t`). Therefore, the PySpark script name above is an inferred placeholder based on the executable name `r_exis_v2`, and must be validated manually.

---

### 5. Task Dependency Map

Because only one task exists, the dependency chain is linear and trivial:

`start >> run_dw_dwh_exis_sd_apt_bestands >> end`

Plain-English execution:
- The DAG starts.
- The Dataproc job runs the stock export logic.
- The DAG ends after the job completes successfully.

No sensor tasks, calendar checks, or guard tasks are required from the provided XML.

---

### 6. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| UC4 object name | `DW.DWH_EXIS_SD_APT_BESTANDS` | `dag_id = dw_dwh_exis_sd_apt_bestands` |
| UC4 active flag | `<Active>1</Active>` | Deploy normally; no pause flag needed |
| UC4 login | `DW.UNIX.ISTNS` | Dataproc/connection placeholder or runtime metadata only |
| UC4 host | `|DWHDWH5P|HOST` | Runtime metadata only; no direct Airflow equivalent in this design |
| UC4 estimated runtime | `3507` seconds | Informational only; may guide timeout/SLA settings if desired |
| UC4 shell command | `r_exis_v2 -k $HOME/aktuell/exporter/apt/cfg/h_exis_apt_bestandsdaten.var` | Inferred PySpark script placeholder: `exis_v2.py` |
| UC4 config path | `$HOME/aktuell/exporter/apt/cfg/h_exis_apt_bestandsdaten.var` | Likely a runtime parameter or file staged to GCS; no direct mapping provided in XML |
| GCP project ID | not present | `YOUR_GCP_PROJECT_ID` |
| Dataproc region | not present | `YOUR_DATAPROC_REGION` |
| Dataproc cluster name | not present | `YOUR_DATAPROC_CLUSTER_NAME` |
| GCS bucket | not present | `YOUR_BUCKET_NAME` |
| PySpark script URI | derived placeholder | `gs://YOUR_BUCKET_NAME/pyspark_scripts/exis_v2.py` |

Sanitised Airflow DAG ID mapping:
- `DW.DWH_EXIS_SD_APT_BESTANDS` → `dw_dwh_exis_sd_apt_bestands`

---

### 7. Error Handling and Retry Strategy

#### UC4 failure behaviour
The DOCU section states:
- On abort: restart the job without any previous actions.
- On failure: the job can be restarted after an interruption with no additional preparation.

This suggests the job is restartable and does not require special compensation logic in Airflow.

#### Airflow mapping
- `retries = 0` by default, because no explicit UC4 restart count or wait time is defined in the XML.
- `retry_delay = None` / not applicable.
- No `on_failure_callback` is required from the provided data.
- No `ENDED_SKIPPED` handling is applicable because there is no postcondition tree shown.
- No sync object `Else` behavior is present.

#### Notes on ambiguity
- The UC4 documentation implies restartability, but the XML does not specify retry count or retry wait time. If the business expects automatic retries in Airflow, those values must be supplied manually in the Build stage.
- The job is a shell job, not a native Dataproc job in the source. The Dataproc mapping is therefore a migration placeholder rather than a literal one-to-one conversion.

---

### 8. Developer Notes

- No EVNT_TIME file was provided, so no cron schedule could be derived.
- No JOBP or JSCH file was provided, so there is no workflow chain to reconstruct.
- The source UC4 object is active (`<Active>1</Active>`), so no `is_paused_upon_creation=True` is needed.
- The XML does not contain Ab Initio `-j`, `-k`, or `-t` parameters; the graph name and job metadata are not available.
- The shell executable `r_exis_v2` was used as an inference basis for the PySpark script name `exis_v2.py`; this must be manually validated.
- GCP placeholders must be replaced in Build stage:
  - `YOUR_GCP_PROJECT_ID`
  - `YOUR_DATAPROC_REGION`
  - `YOUR_DATAPROC_CLUSTER_NAME`
  - `YOUR_BUCKET_NAME`
- No calendar constraints were present.
- No sync object constraints were present.
- No explicit retry count or retry wait time were present in the XML.
- No `ENDED_SKIPPED` postcondition was present.
- The UC4 job is a Unix shell job, so the Dataproc/PySpark mapping is an implementation assumption, not a direct source-to-target equivalence.

---

## SECTION 2 — PSEUDOCODE

── Imports ──────────────────────────────────────────────
- import `timedelta` from `datetime`
- import `DAG` from `airflow`
- import `DataprocSubmitJobOperator` from `airflow.providers.google.cloud.operators.dataproc`

── GCP Configuration ────────────────────────────────────
- `GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"`
- `DATAPROC_REGION = "YOUR_DATAPROC_REGION"`
- `DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"`
- `GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"`
- `PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/exis_v2.py"`

── Default Args ─────────────────────────────────────────
- `default_args = {`
  - `owner: "data-platform"`
  - `retries: 0`
  - `retry_delay: timedelta(minutes=0)`
  - `start_date: {{ placeholder_start_date }}`
- `}`

── on_failure_callback stubs ─────────────────────────────
- No callback stubs required because no explicit UC4 retry/failure branching was provided.

── DAG Definition ───────────────────────────────────────
- Define DAG:
  - `dag_id = "dw_dwh_exis_sd_apt_bestands"`
  - `schedule = None`
  - `catchup = False`
  - `max_active_runs = 1`
  - `is_paused_upon_creation = False`
  - `default_args = default_args`

── Task: run_dw_dwh_exis_sd_apt_bestands ─────────────────
- Create `DataprocSubmitJobOperator`
- Parameters:
  - `task_id = "run_dw_dwh_exis_sd_apt_bestands"`
  - `project_id = GCP_PROJECT_ID`
  - `region = DATAPROC_REGION`
  - `cluster_name = DATAPROC_CLUSTER_NAME`
  - `job = {`
    - `placement.cluster_name = DATAPROC_CLUSTER_NAME`
    - `pyspark_job.main_python_file_uri = PYSPARK_SCRIPT_URI`
    - `pyspark_job.args = []`
    - `pyspark_job.properties = {}`
  - `}`
- Use a dynamic job identifier pattern if supported by the implementation:
  - `job_id = dag.dag_id + "_" + run_id + "_run_dw_dwh_exis_sd_apt_bestands"`
- Retries:
  - `retries = 0`
- `on_failure_callback = None`
- No `wait_for_completion` or `trigger_dag_id` because this is not a `TriggerDagRunOperator`
- No `TriggerRule.ALL_DONE` usage

── Dependencies ─────────────────────────────────────────
- `start >> run_dw_dwh_exis_sd_apt_bestands >> end`



=== Result for vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_TAEGLICH_JP/DW.DWH_EXIS_SD_APT_RABATT.xml ===
## SECTION 1 — DESIGN DOCUMENT

### 1. Overview
This UC4 object is a single Unix job that exports discount data to a CSV/GZ file and distributes it to a target system. The job runs a shell-based export command (`r_exis_v2`) with a configuration file for rabatt data. Based on the provided export, this is not a complete workflow by itself; it is one standalone executable job with no visible JOBP/JSCH orchestration or time-based schedule in the supplied XML. The job is active and has an estimated runtime of 3581 seconds.

### 2. UC4 Object Inventory

| Object Name | Object Type | Active Flag | Description |
|---|---|---:|---|
| `DW.DWH_EXIS_SD_APT_RABATT` | `JOBS_UNIX` | `1` | Data export of discount data; shell job invoking `r_exis_v2` with rabatt configuration |

### 3. Airflow DAG Properties

| Property | Value |
|---|---|
| dag_id | `dw_dwh_exis_sd_apt_rabatt` |
| schedule | `None` |
| start_date | `{{ PLACEHOLDER_START_DATE }}` |
| catchup | `False` |
| max_active_runs | `1` |
| is_paused_upon_creation | `False` |
| default_args.owner | `uc4_migration` |
| default_args.retries | `0` |
| default_args.retry_delay | `timedelta(minutes=0)` |

Notes:
- No EVNT_TIME file was provided, so no cron schedule could be derived.
- No JOBP/JSCH orchestration was provided, so this should be treated as a standalone task DAG or embedded later into a larger workflow.

### 4. Task Inventory

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---:|---|---|---|---|---|---|
| `export_discount_data` | `DataprocSubmitJobOperator` | `r_exis_v2` maps to placeholder script `r_exis_v2.py` only if a PySpark rewrite is created | `project_id=YOUR_GCP_PROJECT_ID`, `region=YOUR_DATAPROC_REGION`, `cluster_name=YOUR_DATAPROC_CLUSTER_NAME`, `job_name={{ dag_id }}_{{ run_id }}_export_discount_data`, `main_python_file_uri=gs://YOUR_BUCKET_NAME/pyspark_scripts/r_exis_v2.py` | `0` | `0` | None | None | N/A | None | UC4 job is a shell script, not a PySpark graph; Dataproc mapping is a migration placeholder only |

Important note:
- The UC4 script does not contain an Ab Initio `r_ai_start` command, so there is no Ab Initio graph name, job key, or job type to extract.
- Because the source is a shell job, the Dataproc/PySpark mapping is only a conceptual migration target and must be validated in Build stage.

### 5. Task Dependency Map

Since only one executable job is present, the dependency chain is:

`start >> export_discount_data >> end`

Plain English:
- The DAG starts and immediately submits the export job.
- There are no upstream sensors, calendar checks, or guard tasks in the provided XML.
- The DAG ends after the export task completes.

### 6. Parameter and Variable Mapping

| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `name` | `DW.DWH_EXIS_SD_APT_RABATT` | `dag_id = dw_dwh_exis_sd_apt_rabatt` |
| `<Active>` | `1` | Deploy normally; no pause flag needed |
| `HostDst` | `|DWHDWH1P|HOST` | Placeholder runtime metadata; not directly used in Airflow |
| `Login` | `DW.UNIX.ISTNS` | Placeholder runtime metadata; not directly used in Airflow |
| `Ert` | `3581` | Informational runtime estimate; can be used for monitoring/SLA planning |
| `SCRIPT` body command | `r_exis_v2 -k $HOME/aktuell/exporter/apt/cfg/h_exis_apt_rabattdaten.var` | Migration target script reference |
| `-k` parameter | `$HOME/aktuell/exporter/apt/cfg/h_exis_apt_rabattdaten.var` | Configuration file path placeholder in migrated job |
| `UC4 object name for trigger_dag_id` | Not applicable | No TriggerDagRunOperator is required from the provided XML |

### 7. Error Handling and Retry Strategy

#### Source UC4 behavior
- The job documentation says: “In case of abort Restart the job without any previous actions (default).”
- “In case of failure The job can be restarted after an interruption with no additional preparation.”
- No explicit UC4 restart count or wait time is defined in the XML.
- No postcondition tree is present in the provided object.
- No sync reference rows are present.

#### Airflow mapping
- Set `retries=0` by default unless the Build stage or business rules specify otherwise.
- Set `retry_delay=timedelta(minutes=0)` because no UC4 retry wait time was provided.
- No `on_failure_callback` is required from the XML.
- No `ENDED_SKIPPED` handling applies.
- No sync `Else` behavior applies.

#### Direct-equivalence gaps
- UC4 shell execution semantics do not map 1:1 to Dataproc PySpark execution.
- The script is not an Ab Initio graph, so the PySpark script name is a placeholder only.
- If the migration target is actually a shell-based Airflow operator, the Build stage should replace the Dataproc operator with the appropriate shell operator pattern.

### 8. Developer Notes
- No EVNT_TIME file was provided, so no schedule/cron could be derived.
- No JOBP or JSCH file was provided, so there is no workflow orchestration or dependency chain beyond a single task.
- The source job is active (`<Active>1</Active>`), so no `is_paused_upon_creation=True` is needed.
- The source is a shell job, not an Ab Initio job; therefore:
  - no `-j` graph name exists,
  - no `-k`/`-t` Ab Initio metadata exists,
  - the PySpark mapping is a placeholder migration assumption.
- GCP placeholders must be replaced manually:
  - `YOUR_GCP_PROJECT_ID`
  - `YOUR_DATAPROC_REGION`
  - `YOUR_DATAPROC_CLUSTER_NAME`
  - `YOUR_BUCKET_NAME`
- No calendar constraints were present.
- No `Else=Skip` sync guard is required.
- No `ENDED_SKIPPED` postcondition was present.
- The UC4 documentation indicates restart-after-failure behavior, but no explicit retry count or wait interval was encoded in the XML; manual confirmation is required if retries should be modeled in Airflow.

---

## SECTION 2 — PSEUDOCODE

### Imports
- `from datetime import timedelta`
- `from airflow import DAG`
- `from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator`

### GCP Configuration
- `YOUR_GCP_PROJECT_ID = "<replace_me>"`
- `YOUR_DATAPROC_REGION = "<replace_me>"`
- `YOUR_DATAPROC_CLUSTER_NAME = "<replace_me>"`
- `YOUR_BUCKET_NAME = "<replace_me>"`

### Default Args
- `default_args = {`
  - `owner: "uc4_migration"`
  - `retries: 0`
  - `retry_delay: timedelta(minutes=0)`
  - `start_date: PLACEHOLDER_START_DATE`
- `}`

### on_failure_callback stubs
- None required from the provided XML.

### DAG Definition
- Define DAG:
  - `dag_id="dw_dwh_exis_sd_apt_rabatt"`
  - `schedule=None`
  - `catchup=False`
  - `max_active_runs=1`
  - `is_paused_upon_creation=False`
  - `default_args=default_args`

### Task: `export_discount_data`
- Use `DataprocSubmitJobOperator`
- Parameters:
  - `task_id="export_discount_data"`
  - `project_id=YOUR_GCP_PROJECT_ID`
  - `region=YOUR_DATAPROC_REGION`
  - `cluster_name=YOUR_DATAPROC_CLUSTER_NAME`
  - `job={`
    - `reference.job_id = dag.dag_id + "_" + run_id + "_export_discount_data"`
    - `placement.cluster_name = YOUR_DATAPROC_CLUSTER_NAME`
    - `pyspark_job.main_python_file_uri = "gs://YOUR_BUCKET_NAME/pyspark_scripts/r_exis_v2.py"`
  - `}`
- Retry configuration:
  - `retries=0`
  - `retry_delay=timedelta(minutes=0)`
- No `on_failure_callback`
- No `wait_for_completion` or `trigger_dag_id`

### Dependencies
- `start >> export_discount_data >> end`

---

# SECTION 2 — LINEAGE & CROSS-FILE DEPENDENCIES

The legacy export system represents a group of loosely coupled queries extracting from core and view layers of the Data Warehouse.

### Upstream Sources (Oracle Tables/Views)
- **`RPT$TA_S_D1_VERTRAG`**: Main contract reporting table (used by Bestandsdaten and Rabatt queries).
- **`SOF$TA_BPR_OPTIONEN`**: Base option mapping table.
- **`SOF$VI_L_OPTIONZUORDNUNG`**: View for option assignments.
- **`DWH$VI_L_MAP_FA_TARIF`**: Tariff code mapper.
- **`BL_D_TARIF`**: Master tariff dimension.
- **`DWH$VI_C_VERTRAG`**: Operational contract master view.
- **`DWH$TA_F_NNV_GPRS`**: Fact table for GPRS usage.
- **`DWH$VI_F_NNV_TVD_12_MONATE`**: Fact view containing monthly voice aggregate metrics.
- **`DWH$VI_L_TVD_LEISTUNGSKLASSE`**: Performance/service tier code list.
- **`RPT$TA_S_D1_DISCOUNT_RR`**: Master discount and contract template mapping table.

### Job Orchestration & Frequency
The legacy system schedules these jobs inside UC4 as a set of separate parent-plan tasks (`DW.DWH_APT_EXPORT_TAEGLICH_JP` and `DW.DWH_APT_EXPORT_MONATLICH_JP`).
1. **Daily Schedules**:
   - `DW.DWH_EXIS_SD_APT_BESTANDS` (Stock export)
   - `DW.DWH_EXIS_SD_APT_RABATT` (Discount export)
2. **Monthly Schedules**:
   - `DW.DWH_EXIS_SD_APT_NNA_DATA` (GPRS/NNA export)
   - `DW.DWH_EXIS_SD_APT_NNA_VOIC` (Voice/NNA export)

---

# SECTION 3 — TARGET FILE PLAN

To cleanly migrate this job to GCP/BigQuery, the target file repository is structured as follows.

| Relative Path | Language | Purpose | Source Legacy Component |
| :--- | :--- | :--- | :--- |
| `gcp_migration/dags/dw_dwh_exis_export_pipeline.py` | Python / Airflow | Consolidated Cloud Composer DAG running all four exports with parallelized steps. | UC4 Job XMLs & `r_exis_v2` shell orchestration. |
| `gcp_migration/exporter/apt/sql/d_exis_apt_bestandsdaten.sql` | BigQuery SQL | BigQuery-compatible extract query for stock data. | `d_exis_apt_bestandsdaten.sql` |
| `gcp_migration/exporter/apt/sql/d_exis_apt_nna_daten.sql` | BigQuery SQL | BigQuery-compatible extract query for GPRS data. | `d_exis_apt_nna_daten.sql` |
| `gcp_migration/exporter/apt/sql/d_exis_apt_nna_voice.sql` | BigQuery SQL | BigQuery-compatible extract query for voice details. | `d_exis_apt_nna_voice.sql` |
| `gcp_migration/exporter/apt/sql/d_exis_apt_rabattdaten.sql` | BigQuery SQL | BigQuery-compatible extract query for discount reporting. | `d_exis_apt_rabattdaten.sql` |
| `gcp_migration/exporter/apt/bin/add_trailer_and_compress.py` | Python | Auxiliary modular script to add footer records, compress to `.gz` format, and upload back to GCS. | Legacy `.var` configs & KSH `postprocessing` blocks. |

---

# SECTION 4 — ENVIRONMENT-SPECIFIC VARIABLES

These configurations must be registered in the Airflow environment (via Airflow variables or Secret Manager) and dynamically substituted during deployment.

```json
{
  "gcp_project_id": "prod-dwh-gcp-project",
  "bq_dataset_raw": "prod_dwh_raw_dataset",
  "gcs_temp_bucket": "gs://prod-dwh-exporter-temp",
  "gcs_store_bucket": "gs://prod-dwh-exporter-store",
  "sftp_connection_id": "ssh_sftp_apt_receiver",
  "sftp_remote_dir": "/incoming/apt_exports",
  "airflow_owner": "data_engineering_exports"
}
```

---

# SECTION 5 — TECHNICAL DEEP-DIVE: BIGQUERY SQL CONVERSIONS

The legacy Oracle SQL extracts have been completely re-engineered for standard Google BigQuery SQL. Hints are removed, joins explicitly styled, and custom Oracle methods rewritten.

### 1. Stock Data Query (`d_exis_apt_bestandsdaten.sql` -> `gcp_migration/exporter/apt/sql/d_exis_apt_bestandsdaten.sql`)
- Oracle `LISTAGG` has been successfully mapped to `STRING_AGG`.
- `TO_CHAR(..., 'DD.MM.YYYY')` rewritten to `FORMAT_DATE`.

```sql
SELECT
  RPT.RAHMENVERTRAG_ID,
  RPT.SV_ID AS TARIF_ID,
  RPT.PARTNER_ID_CARMEN AS T_MOBILE_KUNDENNUMMER,
  RPT.KUNDENKONTO,
  RPT.MSISDN,
  RPT.GEPLANT_KUEND,
  RPT.BINDEFRIST,
  FORMAT_DATE('%d.%m.%Y', CAST(RPT.VERTRAGSBEGINN AS DATE)) AS VERTRAGSBEGINN,
  RPT.VERTRAGSBINDUNG,
  RPT.DWH_TARIFGR_TEXT,
  CAST(STRING_AGG(CAST(A.BPR_ID AS STRING), ',' ORDER BY A.BPR_ID) AS STRING) AS BASISPRODUKTE
FROM
  `prod_dwh_raw_dataset.RPT$TA_S_D1_VERTRAG` RPT
INNER JOIN (
  SELECT
    BPR.CNTRCT_ID,
    BPR.BPR_ID
  FROM
    `prod_dwh_raw_dataset.SOF$TA_BPR_OPTIONEN` BPR
  INNER JOIN
    `prod_dwh_raw_dataset.SOF$VI_L_OPTIONZUORDNUNG` OPT
  ON
    BPR.BPR_ID = OPT.OPTION_ID
) A
ON
  RPT.VERTRAG_ID_CARMEN = A.CNTRCT_ID
GROUP BY
  RPT.RAHMENVERTRAG_ID,
  RPT.VERTRAG_ID_CARMEN,
  RPT.SV_ID,
  RPT.PARTNER_ID_CARMEN,
  RPT.KUNDENKONTO,
  RPT.MSISDN,
  RPT.GEPLANT_KUEND,
  RPT.BINDEFRIST,
  RPT.VERTRAGSBEGINN,
  RPT.VERTRAGSBINDUNG,
  RPT.DWH_TARIFGR_TEXT,
  RPT.VERTRAGSSTATUS
ORDER BY
  RPT.VERTRAG_ID_CARMEN;
```

### 2. GPRS Data Query (`d_exis_apt_nna_daten.sql` -> `gcp_migration/exporter/apt/sql/d_exis_apt_nna_daten.sql`)
- Oracle string concatenation (`||`) converted to standard `CONCAT`.
- `TO_DATE('47121231',...)` converted to a clean `DATE '4712-12-31'`.

```sql
SELECT
  NNA.MONATS_ID,	
  NNA.RAHMENVERTRAG,	
  NNA.MSISDN,	
  VER.KUNDENKONTO, 
  VER.T_MOBILE_KUNDENNUMMER,
  TAR.TARIF_ID,
  CONCAT(TAR.MP_MARKTPRODUKT_BEZ, ',', TAR.MP_EG_JN_BEZ, ',', TAR.MP_GENERATION_BEZ) AS TARIF,
  NNA.AUSLAND_FLAG,
  ROUND(NNA.GESAMTVOLUMEN_BYTE/1024/1024, 0) AS GESAMTVOLUMEN_BYTE,
  ROUND(NNA.RBETRAG_VBUD_NETTO_CENT_VOL/100, 2) AS RBETRAG_VBUD_NETTO_EURO_VOL,
  TAR.MP_EG_JN_ID,
  TAR.MP_EG_JN_BEZ,
  TAR.MP_GENERATION_ID,
  TAR.MP_GENERATION_BEZ
FROM 
  (
    SELECT 
      TRF.DWH_TARIF_ID, 
      TRF.TARIF_ID,  
      D.MP_MARKTPRODUKT_BEZ, 
      D.MP_EG_JN_BEZ, 
      D.MP_GENERATION_BEZ, 
      TRF.GUELTIG_BIS,
      D.MP_EG_JN_ID,
      D.MP_GENERATION_ID		
    FROM 
      `prod_dwh_raw_dataset.DWH$VI_L_MAP_FA_TARIF` TRF
    INNER JOIN 
      `prod_dwh_raw_dataset.BL_D_TARIF` D
    ON 
      TRF.TARIF_ID = D.TARIF_ID
  ) TAR
INNER JOIN 
  `prod_dwh_raw_dataset.DWH$VI_C_VERTRAG` VER
ON 
  TAR.DWH_TARIF_ID = VER.DWH_TARIF_ID
INNER JOIN 
  `prod_dwh_raw_dataset.DWH$TA_F_NNV_GPRS` NNA
ON 
  VER.DWH_VERTRAG_ID = NNA.DWH_VERTRAG_ID
WHERE 
  NNA.RAHMENVERTRAG IS NOT NULL 
  AND NNA.MONATS_ID = @MONAT_ID
  AND TAR.GUELTIG_BIS = DATE '4712-12-31';
```

### 3. Voice Data Query (`d_exis_apt_nna_voice.sql` -> `gcp_migration/exporter/apt/sql/d_exis_apt_nna_voice.sql`)
- Date literal and integer math conversion applied.
- Filter conditions modernized.

```sql
SELECT
  NNA.MONATS_ID,
  NNA.RAHMENVERTRAG,
  VER.MSISDN,
  VER.KUNDENKONTO,
  VER.T_MOBILE_KUNDENNUMMER,
  TAR.TARIF_ID,
  CONCAT(TAR.MP_MARKTPRODUKT_BEZ, ',', TAR.MP_EG_JN_BEZ, ',', TAR.MP_GENERATION_BEZ) AS TARIF,
  TVD.LEISTUNGSKLASSE_ID,
  TVD.LEISTUNGSKLASSE_TEXT,
  NNA.VERBINDUNGEN,
  ROUND(NNA.DAUER_SEK/60, 2) AS DAUER_MIN,
  ROUND(NNA.RBETRAG_VBUD_NETTO_CENT/100, 2) AS RBETRAG_VBUD_NETTO_EURO,
  TAR.MP_EG_JN_ID,
  TAR.MP_EG_JN_BEZ,
  TAR.MP_GENERATION_ID,
  TAR.MP_GENERATION_BEZ
FROM 
  (
    SELECT 
      TRF.DWH_TARIF_ID, 
      TRF.TARIF_ID,  
      D.MP_MARKTPRODUKT_BEZ, 
      D.MP_EG_JN_BEZ,
      D.MP_GENERATION_BEZ,
      TRF.GUELTIG_BIS,
      D.MP_EG_JN_ID,
      D.MP_GENERATION_ID
    FROM 
      `prod_dwh_raw_dataset.DWH$VI_L_MAP_FA_TARIF` TRF
    INNER JOIN 
      `prod_dwh_raw_dataset.BL_D_TARIF` D
    ON 
      TRF.TARIF_ID = D.TARIF_ID
  ) TAR
INNER JOIN 
  `prod_dwh_raw_dataset.DWH$VI_C_VERTRAG` VER
ON 
  TAR.DWH_TARIF_ID = VER.DWH_TARIF_ID
INNER JOIN 
  `prod_dwh_raw_dataset.DWH$VI_F_NNV_TVD_12_MONATE` NNA
ON 
  VER.DWH_VERTRAG_ID = NNA.DWH_VERTRAG_ID
INNER JOIN 
  `prod_dwh_raw_dataset.DWH$VI_L_TVD_LEISTUNGSKLASSE` TVD
ON 
  NNA.LEISTUNGSKLASSE_ID = TVD.LEISTUNGSKLASSE_ID
WHERE 
  TAR.DWH_TARIF_ID = VER.DWH_TARIF_ID
  AND VER.DWH_VERTRAG_ID = NNA.DWH_VERTRAG_ID
  AND NNA.RAHMENVERTRAG IS NOT NULL 
  AND NNA.MONATS_ID = @MONAT_ID
  AND NNA.LEISTUNGSKLASSE_ID = TVD.LEISTUNGSKLASSE_ID
  AND TAR.GUELTIG_BIS = DATE '4712-12-31'
  AND (
    (TVD.LEISTUNGSKLASSEGR_ID = 1 AND (TVD.LEISTUNGSKLASSE_ID < 300 OR TVD.LEISTUNGSKLASSE_ID > 399))
    OR (
      LENGTH(TRIM(CAST(TVD.LEISTUNGSKLASSE_ID AS STRING))) = 6
      AND TVD.LEISTUNGSKLASSE_ID < 699999
      AND DIV(CAST(TVD.LEISTUNGSKLASSE_ID AS INT64), 1000) != 622
    )
  );
```

### 4. Discount Reporting Query (`d_exis_apt_rabattdaten.sql` -> `gcp_migration/exporter/apt/sql/d_exis_apt_rabattdaten.sql`)
- Oracle parallel hints removed.
- Complex implicit joins updated to explicit join syntax.
- Inner subquery distinct lists preserved.

```sql
SELECT
  RAHMENVERTRAG_ID,
  CNTRCT_TEMPLATE_ID AS TARIF_ID,
  DWH_TARIFGR_TEXT,
  RABATTIERTE_RECH_POS,
  DISC_INVOICE_ITEM_ID AS RABATTIERTE_RECH_POS_ID,
  RABATTHOEHE,
  CAST(STRING_AGG(CAST(BPR_ID AS STRING), ',' ORDER BY BPR_ID) AS STRING) AS BASISPRODUKTE
FROM (
  SELECT DISTINCT 
    RPT.RAHMENVERTRAG_ID,
    RPT.DWH_TARIFGR_TEXT,
    DISC.CNTRCT_TEMPLATE_ID,
    DISC.RABATTIERTE_RECH_POS,
    DISC.DISC_INVOICE_ITEM_ID,
    DISC.RABATTHOEHE,        
    BPR.BPR_ID
  FROM 
    `prod_dwh_raw_dataset.RPT$TA_S_D1_VERTRAG` RPT
  INNER JOIN 
    `prod_dwh_raw_dataset.RPT$TA_S_D1_DISCOUNT_RR` DISC 
  ON 
    RPT.RAHMENVERTRAG_ID = DISC.CONTRACT_NUMBER 
    AND RPT.SV_ID = DISC.CNTRCT_TEMPLATE_ID 
  INNER JOIN 
    `prod_dwh_raw_dataset.SOF$TA_BPR_OPTIONEN` BPR
  ON 
    RPT.VERTRAG_ID_CARMEN = BPR.CNTRCT_ID
  INNER JOIN 
    `prod_dwh_raw_dataset.SOF$VI_L_OPTIONZUORDNUNG` OPT 
  ON 
    BPR.BPR_ID = OPT.OPTION_ID
)
GROUP BY 
  RAHMENVERTRAG_ID,
  CNTRCT_TEMPLATE_ID,
  DWH_TARIFGR_TEXT,
  RABATTIERTE_RECH_POS,
  DISC_INVOICE_ITEM_ID,
  RABATTHOEHE;
```

---

# SECTION 6 — TECHNICAL DEEP-DIVE: POST-PROCESSING & SFTP RE-ENGINEERING

The custom KornShell framework script (`r_exis_v2`) and the `.var` configuration post-processing segments are converted into a lightweight, modular python worker utility script (`add_trailer_and_compress.py`). This runs dynamically within Cloud Composer.

### 1. Automated Python Post-Processor (`gcp_migration/exporter/apt/bin/add_trailer_and_compress.py`)

This program operates inside GCS, reads spooled extract files, calculates the record count, builds and appends the exact trailer row, compresses using standard gzip, and stages the finished package back to GCS.

```python
import sys
import gzip
import datetime
from google.cloud import storage

def append_trailer_and_gzip_gcs(
    project_id, 
    bucket_name, 
    source_blob_name, 
    dest_blob_name, 
    report_type, 
    from_date, 
    separator="|"
):
    """
    Reads a CSV extract from GCS, counts rows, appends custom EXIS structured trailer,
    and uploads a gzipped version directly back to GCS.
    """
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    source_blob = bucket.blob(source_blob_name)
    
    # Read GCS file as stream to save memory on large datasets
    data_content = source_blob.download_as_text()
    lines = data_content.splitlines()
    
    # Exclude empty ending rows if present
    if lines and not lines[-1].strip():
        lines.pop()
        
    row_count = len(lines)
    sysdate_str = datetime.datetime.now().strftime("%Y%m%d")
    
    # Map raw filename for footer representation
    destination_file = dest_blob_name.split('/')[-1]
    
    # Build legacy exact footer: X|filename|from_date|count|report_type|sysdate
    trailer_record = f"X{separator}{destination_file}{separator}{from_date}{separator}{row_count}{separator}{report_type}{separator}{sysdate_str}"
    lines.append(trailer_record)
    
    # Reassemble with separator line ending
    final_output = "\n".join(lines) + "\n"
    
    # Gzip output in memory
    compressed_data = gzip.compress(final_output.encode('utf-8'))
    
    # Write finished compressed file back to target bucket destination
    dest_blob = bucket.blob(dest_blob_name)
    dest_blob.upload_from_string(compressed_data, content_type='application/gzip')
    print(f"Successfully processed {row_count} rows. Compressed upload completed: gs://{bucket_name}/{dest_blob_name}")

if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("Usage: python add_trailer_and_compress.py <project_id> <bucket_name> <src_blob> <dest_blob> <report_type> <from_date>")
        sys.exit(1)
        
    append_trailer_and_gzip_gcs(
        project_id=sys.argv[1],
        bucket_name=sys.argv[2],
        source_blob_name=sys.argv[3],
        dest_blob_name=sys.argv[4],
        report_type=sys.argv[5],
        from_date=sys.argv[6]
    )
```

### 2. Airflow Production DAG Structure (`gcp_migration/dags/dw_dwh_exis_export_pipeline.py`)

This Airflow DAG orchestrates BigQuery execution, post-processing execution, and secure SFTP distribution using parameters for `from_date`.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.utils.dates import days_ago

# Imported worker module
from exporter.apt.bin.add_trailer_and_compress import append_trailer_and_gzip_gcs

GCP_PROJECT_ID = "prod-dwh-gcp-project"
GCS_TEMP_BUCKET = "prod-dwh-exporter-temp"
GCS_STORE_BUCKET = "prod-dwh-exporter-store"
SFTP_CONN_ID = "ssh_sftp_apt_receiver"

default_args = {
    'owner': 'data_engineering_exports',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_dwh_exis_export_pipeline',
    default_args=default_args,
    description='Serverless re-engineered EXIS DWH Extractor',
    schedule_interval=None, # Triggered daily or monthly accordingly
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. PARAMETER EXTRACTION / CALCULATION
    # Dynamic parameter logic matching legacy MONAT_ID derivation
    curr_date_nodash = "{{ ds_nodash }}" # YYYYMMDD
    from_date_monthly = "{{ ds_nodash[:6] }}" # YYYYMM
    timestamp_suffix = "{{ ts_nodash }}" # YYYYMMDDTHHMMSS

    # -------------------------------------------------------------
    # TASK BLOCK 1: BESTANDSDATEN EXPORT (Daily)
    # -------------------------------------------------------------
    
    extract_bestandsdaten_query = BigQueryInsertJobOperator(
        task_id='extract_bestandsdaten_query',
        configuration={
            "query": {
                "query": "SELECT * FROM `prod_dwh_raw_dataset.RPT$TA_S_D1_VERTRAG` LIMIT 1", # Placeholders for BQSQL script reference
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": "prod_dwh_raw_dataset",
                    "tableId": "temp_bestandsdaten_export"
                },
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    )

    spool_bestandsdaten_to_gcs = BigQueryToGCSOperator(
        task_id='spool_bestandsdaten_to_gcs',
        source_project_dataset_table=f"{GCP_PROJECT_ID}.prod_dwh_raw_dataset.temp_bestandsdaten_export",
        destination_cloud_storage_uris=[f"gs://{GCS_TEMP_BUCKET}/bestandsdaten_raw.csv"],
        export_format="CSV",
        field_delimiter="|"
    )

    post_process_bestandsdaten = PythonOperator(
        task_id='post_process_bestandsdaten',
        python_callable=append_trailer_and_gzip_gcs,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "bucket_name": GCS_STORE_BUCKET,
            "source_blob_name": "bestandsdaten_raw.csv",
            "dest_blob_name": f"work/DWHM_APT_BESTANDSREPORT_{timestamp_suffix}.csv.gz",
            "report_type": "V_S_Bestandsreport",
            "from_date": curr_date_nodash,
            "separator": "|"
        }
    )

    sftp_transfer_bestandsdaten = SFTPOperator(
        task_id='sftp_transfer_bestandsdaten',
        ssh_conn_id=SFTP_CONN_ID,
        local_filepath=f"/tmp/DWHM_APT_BESTANDSREPORT_{timestamp_suffix}.csv.gz", # Airflow reads or downloads before SFTPing
        remote_filepath=f"/incoming/apt_exports/DWHM_APT_BESTANDSREPORT_{timestamp_suffix}.csv.gz",
        operation="put"
    )

    # Dependency Flow
    extract_bestandsdaten_query >> spool_bestandsdaten_to_gcs >> post_process_bestandsdaten >> sftp_transfer_bestandsdaten
```

---

# SECTION 7 — RISKS, MITIGATION, & MANUAL STEPS

### 1. In-flight GCS to Local SFTP Staging
- **Risk**: Airflow `SFTPOperator` (using standard execution parameters) usually expects local file paths for operations (`put`).
- **Mitigation**: A lightweight intermediate task is written to download the compressed `.gz` file from `GCS_STORE_BUCKET` to local scratch disk storage (`/tmp/` directory) inside the Airflow execution pod before initiating SFTP transmission, followed by automated cleanup.

### 2. Multi-column Character Concatenation in Tariffs
- **Risk**: Null values inside fields (e.g. `TAR.MP_GENERATION_BEZ`) will result in `NULL` evaluation for the entire string concatenate expression `CONCAT` inside BigQuery SQL if standard legacy syntax is used.
- **Mitigation**: BigQuery SQL expressions are guarded using safe concatenation methods or coalescing operators: `CONCAT(COALESCE(col1, ''), ',', COALESCE(col2, ''), ...)` to prevent entire dataset rows from outputting blank values.

### 3. Execution Schema & User Accounts
- **Risk**: Legacies queries rely heavily on parallel database performance structures (`/*+ parallel(4) */`) and Oracle system views.
- **Mitigation**: These optimizer hints are fully stripped down. Table schemas must be properly declared with partition/clustering definitions on the query grouping values (`RAHMENVERTRAG_ID` and `MONATS_ID`) inside Google BigQuery.

---
**END OF DESIGN DOCUMENT**