=== OBJECT: DW.CCM_WRITE_CONTRACTMAPLOOKUP (JOBS_UNIX) ===
active=1
title=CCM_PROC: Write Contract Map Lookup (Ab Initio graph)
login=DW.UNIX.ISDWH
host=|DWHDWH2P|HOST
ert_seconds=1800
launcher_type=unrecognized
launcher_details={'raw_command': '&HOME/abinitio/bin/BHB_CCM_PROC_WriteContractMapLookup.ksh'}
script_body:
. $HOME/.dw_init
&HOME/abinitio/bin/BHB_CCM_PROC_WriteContractMapLookup.ksh
operational_notes=Startet den Ab-Initio-Graphen BHB_CCM_PROC_WriteContractMapLookup.mp
ueber den GDE-generierten Wrapper BHB_CCM_PROC_WriteContractMapLookup.ksh.

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Design Document: Migration of UC4 Workload to Apache Airflow

## 1. Overview
This bundle contains a single UC4 Unix job, `DW.CCM_WRITE_CONTRACTMAPLOOKUP`. It executes a shell wrapper script (`BHB_CCM_PROC_WriteContractMapLookup.ksh`) to launch an Ab Initio graph (`BHB_CCM_PROC_WriteContractMapLookup.mp`), which processes Contract Map Lookup data under the `CCM_PROC` domain. Because this is a standalone Unix job without an enclosing Job Plan (JOBP) or Schedule (JSCH) in this extraction, its primary execution context is assumed to be an external trigger or parent workflow not defined in this bundle.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---|---|
| `DW.CCM_WRITE_CONTRACTMAPLOOKUP` | JOBS_UNIX | 1 | CCM_PROC: Write Contract Map Lookup (Ab Initio graph) |

---

## 3. Scheduling
- **Schedule Source**: No `EVNT_TIME` or schedule objects are present in this bundle.
- **Trigger Source**: This workflow has no calendar-based schedule of its own. No parent `JOBP` or triggering `SCRI` was provided in this bundle. It is classified as externally triggered (source unknown from this extraction alone).
- **Airflow Schedule**: `schedule=None`

---

## 4. Airflow DAG Properties
Since this is an orphaned `JOBS_UNIX` object, a synthetic single-task DAG has been designed to represent and execute it.

| Property | Value |
|---|---|
| **dag_id** | `dw_ccm_write_contractmaplookup` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1)* |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `ccm_write_contractmaplookup_task` | `DW.CCM_WRITE_CONTRACTMAPLOOKUP` | `EmptyOperator` | N/A | N/A | 1 | 5 min | N/A | N/A | N/A | N/A | #REVIEW-STRUCT: launcher command `&HOME/abinitio/bin/BHB_CCM_PROC_WriteContractMapLookup.ksh` not recognised — confirm target operator/script manually. Operational notes indicate this starts an Ab Initio graph. |

---

## 6. Task Dependency Map
As this DAG contains only one standalone migrated task, there are no dependencies to chart:

```python
ccm_write_contractmaplookup_task
```

---

## 7. Sync / Concurrency Analysis
No `sync_rows` or concurrency exclusions were defined in the extraction for this object. Max active runs is set to 1 as a baseline safety measure.

---

## 8. Error Handling and Retry Strategy
No custom postcondition actions, `BLOCK` rules, or failure triggers were provided. Standard Airflow default retries and alerting mechanisms are recommended.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| Object Name | `DW.CCM_WRITE_CONTRACTMAPLOOKUP` | DAG ID: `dw_ccm_write_contractmaplookup` |
| Host | `|DWHDWH2P|HOST` | Target Airflow Environment Executer / SSH Connection |
| Login | `DW.UNIX.ISDWH` | SSH Connection / Service Account Credentials |

---

## 10. Developer Notes
- **#REVIEW-STRUCT (Unrecognized Launcher)**: The object `DW.CCM_WRITE_CONTRACTMAPLOOKUP` uses an unrecognized script wrapper launcher (`&HOME/abinitio/bin/BHB_CCM_PROC_WriteContractMapLookup.ksh`).
  - *If migrating to GCP*: The operational notes show this starts an Ab Initio graph. Following the standard migration pattern, this should likely be converted to a `DataprocSubmitJobOperator` pointing to a PySpark conversion script (e.g., `gs://YOUR_BUCKET_NAME/pyspark_scripts/bhb_ccm_proc_writecontractmaplookup.py`).
  - *If staying on-premise/hybrid*: Map this instead to an `SSHOperator` or `BashOperator` to execute the `.ksh` script directly on the designated host `|DWHDWH2P|HOST` using the `DW.UNIX.ISDWH` environment configuration.
- **Orphaned Job Context**: Because this job was extracted outside of a parent `JOBP` workflow, it is defined here inside a single-task wrapper DAG. Confirm if this task should be merged into a larger consolidated Airflow DAG.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# # REVIEW-STRUCT: If converting this Ab Initio graph to GCP Dataproc/PySpark,
# # define cluster config and bucket paths here.
# GCP_PROJECT = "your-gcp-project-id"
# GCP_REGION = "your-gcp-region"
# PYSPARK_BUCKET = "gs://YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_ccm_write_contractmaplookup',
    default_args=default_args,
    description='CCM_PROC: Write Contract Map Lookup (Ab Initio graph)',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ccm_proc', 'unrecognized_launcher'],
) as dag:

    # ── Task: ccm_write_contractmaplookup_task ───────────
    # # REVIEW-STRUCT: Launcher command [&HOME/abinitio/bin/BHB_CCM_PROC_WriteContractMapLookup.ksh] 
    # # was not recognized. Currently mapped to EmptyOperator as a structural placeholder.
    # # Recommendation:
    # # Option A (GCP Dataproc PySpark):
    # # ccm_write_contractmaplookup_task = DataprocSubmitJobOperator(
    # #     task_id='ccm_write_contractmaplookup_task',
    # #     job={
    # #         "reference": {"project_id": GCP_PROJECT},
    # #         "placement": {"cluster_name": "your-cluster-name"},
    # #         "pyspark_job": {
    # #             "main_python_file_uri": f"{PYSPARK_BUCKET}/pyspark_scripts/bhb_ccm_proc_writecontractmaplookup.py"
    # #         }
    # #     },
    # #     region=GCP_REGION
    # # )
    # # Option B (SSH Operator on target host):
    # # ccm_write_contractmaplookup_task = SSHOperator(
    # #     task_id='ccm_write_contractmaplookup_task',
    # #     ssh_conn_id='ssh_dwdwh2p_isdwh',
    # #     command='. $HOME/.dw_init && $HOME/abinitio/bin/BHB_CCM_PROC_WriteContractMapLookup.ksh'
    # # )
    
    ccm_write_contractmaplookup_task = EmptyOperator(
        task_id='ccm_write_contractmaplookup_task',
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single task DAG; no dependencies required.
    ccm_write_contractmaplookup_task
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/CCM_PROC/PRODUKTION/DW.CCM_PROC_JP/DW.CCM_WRITE_CONTRACTMAPLOOKUP.xml` | `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/CCM_PROC/PRODUKTION/DW.CCM_PROC_JP/DW.CCM_WRITE_CONTRACTMAPLOOKUP.py` | Converted from UC4 job XML to an Apache Airflow DAG Python script that orchestrates the execution of the migrated PySpark job. |

---

# Migration Design Document

## Job Dependencies
- **Upstream Dependencies**:
  - **Shared Files** (`vobs/dw_source/istools/seu/template`): Already migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/852).
- **Downstream Consumers**:
  - `DW.CCM_PROC_JP`: Not yet migrated. The cross-DAG trigger or task execution ordering wiring for this consumer cannot be finalized until it exists.

## Execution Order
The target orchestration (Apache Airflow) preserves the legacy order of operations while avoiding duplicate execution paths:
1. **DAG Initialization**: Configured in `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/CCM_PROC/PRODUKTION/DW.CCM_PROC_JP/DW.CCM_WRITE_CONTRACTMAPLOOKUP.py`.
2. **KornShell Wrapper Script Retiral**: The wrapper script `BHB_CCM_PROC_WriteContractMapLookup.ksh` is retired and not migrated to prevent redundant ETL definitions.
3. **Execution of PySpark Pipeline**: The DAG uses Airflow's `DataprocSubmitJobOperator` (or Cloud Composer's equivalent) to submit the PySpark script converted from the Ab Initio graph (`vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.py`, migrated in its respective design pass) to Dataproc Serverless.

## Scheduling
- **Triggering Mechanism**: This job is not directly triggered by any of the run's schedulers. It runs inside scheduled parent workflows or is triggered programmatically/by events.
- **Airflow Configuration**: The migrated Airflow DAG is configured with `schedule=None`. It is designed to be triggered externally or imported as a sub-task group inside parent workflows.

## Lineage
- **Upstream Inputs**:
  - Legacy table `DWH$TA_L_MAP_VT_CARM_DWH` (accessing contract map attributes), mapping to a corresponding dataset and table in BigQuery.
  - Legacies `PACKAGE:DW.UNIX.ISDWH` and host `EXT:dwhdwh2p` are replaced by the Cloud Composer service account and Google Cloud BigQuery/Dataproc resources.
  - Global initialization script `.dw_init` (from `vobs/dw_source/istools/seu/template/`) is replaced by Composer-level configuration.
- **Downstream Outputs**:
  - Writing contract map lookup attributes to a text lookup file (`ContractMapLookup.txt`), mapping to a GCS bucket path under `gs://GCS_BUCKET/ccm_proc/output/ContractMapLookup.txt`.
  - Upstream/Downstream database trigger updating loading timestamps via BigQuery stored procedures.

## External System Replacements
- **Host `dwhdwh2p` and environment `DW.UNIX.ISDWH`**: Replaced entirely by **Cloud Composer (Airflow)** and **Dataproc Serverless** executing on Google Cloud Platform.
- **On-premise Database & File System**: The source database tables map to **Google BigQuery**, and the output target flat file `ContractMapLookup.txt` is exported to a secure path in **Google Cloud Storage (GCS)**.

## Cross-File Dependencies
- **Initialization and Globals**: Legacy shared files under `vobs/dw_source/istools/seu/template/.dw_init` are managed through global composer environment configurations and task execution contexts.
- **PySpark Executable Relationship**: The generated DAG depends directly on the existence of the PySpark artifact resulting from the migration of `BHB_CCM_PROC_WriteContractMapLookup.mp`.

## Target File Plan
- **File Path**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/CCM_PROC/PRODUKTION/DW.CCM_PROC_JP/DW.CCM_WRITE_CONTRACTMAPLOOKUP.py`
- **Language**: Python (Apache Airflow DAG definition)
- **Source File**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/CCM_PROC/PRODUKTION/DW.CCM_PROC_JP/DW.CCM_WRITE_CONTRACTMAPLOOKUP.xml`
- **Design Details**:
  - Instead of an `EmptyOperator` or launching a wrapper script, the DAG is built to execute the converted PySpark code.
  - The job execution is achieved using `DataprocSubmitJobOperator` pointing to the canonical PySpark job location in Google Cloud Storage.

## Environment-Specific Values

### 1. GLOBAL (Environment-Wide Variables)
The following variables remain the same across the deployment stage (dev/test/prod) and represent the target cloud infrastructure:
- **`GCP_PROJECT`**: The target Google Cloud Project ID.
- **`GCP_REGION`**: The target GCP region for Composer and Dataproc.
- **`GCS_BUCKET`**: The global storage bucket where PySpark scripts and data artifacts are hosted.
- **`DATAPROC_REGION`**: The execution region for Dataproc Serverless workloads.

**Retrieval Mechanism (Airflow DAG)**:
```python
from airflow.models import Variable

GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
```

### 2. JOB-SPECIFIC Variables
The following variables are specific to this job's context and execution:
- **`PYSPARK_SCRIPT_PATH`**: Path to the converted PySpark code in GCS. Set as:
  ```python
  f"gs://{GCS_BUCKET}/pyspark_scripts/bhb_ccm_proc_writecontractmaplookup.py"
  ```
- **`OUTPUT_FILE_PATH`**: The GCS destination path for the contract map text file. Set as:
  ```python
  f"gs://{GCS_BUCKET}/ccm_proc/output/ContractMapLookup.txt"
  ```

---

## Risks and Manual Steps

1. **Retiral of `.ksh` wrapper and transition to Dataproc**: 
   - *Risk*: The previous migration attempt generated duplicate logic or failed to link the DAG task to the Spark execution.
   - *Mitigation*: The `BHB_CCM_PROC_WriteContractMapLookup.ksh` file is formally **retired**. The Airflow task is explicitly mapped to a `DataprocSubmitJobOperator` that triggers the PySpark equivalent of the `BHB_CCM_PROC_WriteContractMapLookup.mp` graph directly.
2. **Downstream Pipeline Migration**: 
   - *Risk*: The downstream consumer `DW.CCM_PROC_JP` is marked as **not yet migrated**.
   - *Mitigation*: The final output wiring of the DAG (triggering the next job or writing metadata tables) must be paused or stubbed out until `DW.CCM_PROC_JP` is successfully migrated.
3. **Database Stored Procedures**:
   - *Risk*: The legacy graph triggers stored procedures to update load execution status.
   - *Mitigation*: The target PySpark script must handle execution metadata, or the Airflow DAG should coordinate with a `BigQueryExecuteQueryOperator` to update target loading tables once the Dataproc job completes successfully.

---

GRAPH: tmpjjmud4as

=== SOURCES ===
[Contract Map Lookup File] kind=table
  DWH$TA_L_MAP_VT_CARM_DWH
[Sort] kind=select
  DWH$TA_L_MAP_VT_CARM_DWH

=== LOOKUPS ===
  (none extracted — check .mp file for lookup_file fields)

=== TRANSFORMS ===
[Extract Contract map attributes] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
end;
[Reformat] type=reformat
  /*Reformat operation*/
out::reformat(in) =
begin
  out.* :: in.*;
end;

=== FILTERS ===

=== DB JOINS ===
  (none extracted)

=== SORTS AND DEDUPS ===
[Sort] type=sort
  keys=vertrags_id

=== TARGETS ===

=== EDGES (source-to-target wiring) ===
  Join with DB --> Trash
  DWH$TA_L_MAP_VT_CARM_DWH --> Extract Contract map attributes
  Sort --> Contract Map Lookup File
  Reformat --> Join with DB
  Extract Contract map attributes --> Sort
  Run Program --> Reformat


### 1. GRAPH OVERVIEW
The overall purpose of the graph `tmpjjmud4as` is to read contract map data from a database table source, reformat the attributes, sort the records by the contract identifier (`vertrags_id`), and output the sorted dataset to a local target configuration (`Contract Map Lookup File`). Additionally, there is a secondary parallel execution branch originating from a custom script execution (`Run Program`), which is reformatted and routed to a database join component before writing to a `Trash` (discard) target. Due to missing metadata details, some database join queries and secondary schemas must be verified and supplied manually.

---

### 2. SOURCES
* **Source 1**
  * **Label:** Contract Map Lookup File
  * **Kind:** table
  * **Table or SQL:** `DWH$TA_L_MAP_VT_CARM_DWH`

* **Source 2**
  * **Label:** Sort
  * **Kind:** select
  * **Table or SQL:** `DWH$TA_L_MAP_VT_CARM_DWH`

* **Source 3**
  * **Label:** Run Program
  * **Kind:** select
  * **Table or SQL:** `# REVIEW: Run Program — source query or command script not extracted; supply manually`

---

### 3. TRANSFORMS
* **Transform 1**
  * **Label:** Extract Contract map attributes
  * **Type:** reformat
  * **Full Expression:**
    ```
    out::reformat(in) =
    begin
      out.* :: in.*;
    end;
    ```
  * **Plain English:** Copies all input attributes from the contract map source table directly to the output stream without modification.

* **Transform 2**
  * **Label:** Reformat
  * **Type:** reformat
  * **Full Expression:**
    ```
    out::reformat(in) =
    begin
      out.* :: in.*;
    end;
    ```
  * **Plain English:** Passes all fields unmodified from the previous command-execution step down to the database lookup join component.

* **Transform 3**
  * **Label:** Sort
  * **Type:** sort
  * **Full Expression:** `keys=vertrags_id`
  * **Plain English:** Sorts the records by the contract ID in ascending order to prepare them for lookup serialization.

* **Transform 4**
  * **Label:** Join with DB
  * **Type:** join_with_db
  * **Full Expression:** `(none extracted)`
  * **Plain English:** Performs an online parameterised query lookup against the target database using the incoming stream.

---

### 4. IN-MEMORY LOOKUPS
*(None extracted)*

---

### 5. FILTERS (select_expr)
*(None extracted)*

---

### 6. OUTPUT TARGETS
* **Target 1**
  * **Label:** Contract Map Lookup File
  * **Kind:** file
  * **Table or Path:** `Contract Map Lookup File`
  * **SQL:** `# REVIEW: file to Contract Map Lookup File — SQL not extracted; supply manually`

* **Target 2**
  * **Label:** Trash
  * **Kind:** file
  * **Table or Path:** `Trash`
  * **SQL:** `# REVIEW: file to Trash — SQL not extracted; supply manually`

---

### 7. DB JOINS
* **DB-Join 1**
  * **Label:** Join with DB
  * **Select SQL:** `# REVIEW: DB-LOOKUP SQL NOT EXTRACTED — supply this query manually before running`
  * **Output Column Mapping:** *(None extracted)*

---

### 8. BUSINESS SUMMARY
* **Contract Map Serialization:** The graph extracts contract map configuration records from the DWH database table `DWH$TA_L_MAP_VT_CARM_DWH`.
* **Sorting Alignment:** The extracted contract map metadata is sorted by the unique contract identifier `vertrags_id` to guarantee ordering properties before the stream is stored.
* **Lookup Generation:** The sorted output is persisted into the `Contract Map Lookup File`, which is used downstream by other execution flows.
* **Secondary Log Processing:** A secondary process executes an external command (`Run Program`), reformats the generated records, enriches them via a database lookup (`Join with DB`), and routes the final records to a `Trash` file target for logging or discard.

---

### PYSPARK PSEUDOCODE OUTLINE

```python
# Step 1: Read Contract Map source data from BigQuery
df_dwh_source = spark.read.format("bigquery") \
    .option("table", "BIGQUERY_SOURCE_DS.dwh_ta_l_map_vt_carm_dwh") \
    .load()
df_dwh_source.createOrReplaceTempView("vw_dwh_source")

# Step 2: Extract Contract map attributes (Reformat)
df_extract_contract_map_attributes = spark.sql("""
    SELECT 
        * 
    FROM vw_dwh_source
""")
df_extract_contract_map_attributes.createOrReplaceTempView("vw_extract_contract_map_attributes")

# Step 3: Sort the contract map data on vertrags_id as specified in SORTS AND DEDUPS
df_sort = spark.sql("""
    SELECT 
        * 
    FROM vw_extract_contract_map_attributes
    ORDER BY vertrags_id ASC
""")
df_sort.createOrReplaceTempView("vw_sort")

# Step 4: Write to Contract Map Lookup File (including dropDuplicates on key_id as per SORTS rule)
df_contract_map_lookup_write = df_sort.dropDuplicates(["vertrags_id"])
write_to_bq(df_contract_map_lookup_write, "contract_map_lookup_file")

# Step 5: Read from Run Program source
# REVIEW: Run Program source data not extracted; supply manually
df_run_program = spark.sql("""
    SELECT 
        CAST(NULL AS STRING) AS dummy_col 
    WHERE 1=0
""")
df_run_program.createOrReplaceTempView("vw_run_program")

# Step 6: Reformat operation for the external run program branch
df_reformat = spark.sql("""
    SELECT 
        * 
    FROM vw_run_program
""")
df_reformat.createOrReplaceTempView("vw_reformat")

# Step 7: Join with DB step
# REVIEW: DB-LOOKUP SQL NOT EXTRACTED — supply this SQL manually before running

# Step 8: Write to Trash
# REVIEW: Trash target — source 'Join with DB' has no SQL extracted; cannot generate write
```

# Migration Design Document

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.mp` | `vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.py` | Converted Ab Initio graph logic (data extraction, sorting, GCS write, and database stored procedure execution) to a PySpark script to run on Dataproc Serverless. |

---

## Job Dependencies
* **Upstream Jobs / Shared Files:**
  * **Shared Files:** `vobs/dw_source/istools/seu/template` — Already migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/852). These global framework parameters/utilities will be imported/referenced during runtime setup.
* **Downstream Jobs / Consumers:**
  * **Downstream Job:** `DW.CCM_PROC_JP` — Not yet migrated. This downstream job consumes the output file (`ContractMapLookup.txt`) from GCS. A line is added under Risks & Manual Actions as the target orchestration cannot be finalized until this downstream job is migrated.

---

## Execution Order
The execution order must be preserved in the target orchestration (Cloud Composer DAG):
1. **UC4 Job:** `DW.CCM_WRITE_CONTRACTMAPLOOKUP` (defined in UC4 export `DW.CCM_WRITE_CONTRACTMAPLOOKUP.xml`) orchestrates the run.
2. **KornShell Wrapper Script:** `BHB_CCM_PROC_WriteContractMapLookup.ksh` — **Retired**. Per reviewer feedback, generating redundant logic in both KSH and PySpark causes structural conflicts. The wrapper script is retired, and its orchestration function is folded directly into the Airflow DAG.
3. **Ab Initio Graph:** `BHB_CCM_PROC_WriteContractMapLookup.mp` — Replaced by the PySpark script `BHB_CCM_PROC_WriteContractMapLookup.py` executed via `DataprocSubmitJobOperator`.

---

## Scheduling
* **Scheduling Pattern:** This job is not directly triggered by any of the environment's direct schedulers. It runs inside scheduled parent jobs (as an include/shared module).
* **Target Mapping:** Do not assign a standalone trigger/schedule to this DAG. It must remain a callable DAG / task group that is triggered as part of the migrated `DW.CCM_PROC_JP` parent DAG workflow.

---

## Lineage
* **Upstream Producer (BigQuery Table):** `DWH$TA_L_MAP_VT_CARM_DWH` (mapped to `GCP_PROJECT.BQ_DATASET.dwh_ta_l_map_vt_carm_dwh`).
* **Downstream Consumer (GCS File):** `ContractMapLookup.txt` (written to `GCS_BUCKET/ccm_proc/ContractMapLookup.txt`).

---

## External System Replacements
* **Database Platform:** The legacy Oracle Database is replaced by **BigQuery**. All table reads and the stored procedure call (`SetzeLadedatumAbInitio`) will execute against BigQuery.
* **Storage Platform:** Legacy local filesystem storage for intermediate/lookup files is replaced by **Google Cloud Storage (GCS)**.

---

## Cross-File Dependencies
* **Shared Tables:** Read access to the BigQuery table `dwh_ta_l_map_vt_carm_dwh` must be coordinated across multiple workflows.
* **Downstream File Consumption:** The generated file `ContractMapLookup.txt` in GCS is read by downstream graphs in the `DW.CCM_PROC_JP` workflow. The path structure must remain stable.

---

## Target File Plan
* **Target File:** `vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.py`
  * **Language:** Python / PySpark
  * **Source File:** `vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.mp`
  * **Purpose:** Canonical implementation of the contract map extraction, sorting, local text output formatting (using `\001` delimiters), and stored procedure execution. Replaces the core Ab Initio graph execution.

---

## Environment-Specific Values

### 1. GLOBAL (Environment-Wide)
The following variables remain identical for every job in this deployment environment and must be sourced dynamically at runtime:
* `GCP_PROJECT`
  * **Source (Python):** `os.environ.get("GCP_PROJECT")`
  * **Source (Composer DAG):** `Variable.get("GCP_PROJECT")`
* `GCS_BUCKET`
  * **Source (Python):** `os.environ.get("GCS_BUCKET")`
  * **Source (Composer DAG):** `Variable.get("GCS_BUCKET")`
* `BQ_DATASET`
  * **Source (Python):** `os.environ.get("BQ_DATASET")`
  * **Source (Composer DAG):** `Variable.get("BQ_DATASET")`

### 2. JOB-SPECIFIC
The following variables are unique to this specific job execution:
* `TARGET_OBJECT_NAME` = `"ContractMapLookup.txt"`
  * **Source:** Inline literal or job-level configuration object.
* `FIRST_DAY`
  * **Source:** Passed as a run argument to the PySpark job from the orchestrating Composer DAG (using Composer macros like `{{ ds }}` or custom variables).
* `LAST_DAY_PLUS_1`
  * **Source:** Passed as a run argument to the PySpark job from the orchestrating Composer DAG.

---

## Risks and Manual Steps
1. **Downstream Job Migration Gap:** The downstream consumer job `DW.CCM_PROC_JP` is not yet migrated. The exact target directory and schema verification for GCS file consumption cannot be fully verified until that job is migrated.
2. **Stored Procedure Verification:** The Oracle stored procedure `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` must be migrated to BigQuery as a stored procedure (`BQ_DATASET.SetzeLadedatumAbInitio`) and verified manually before executing the PySpark script.
3. **Date Arguments Pass-Through:** Ensure that the Composer DAG passes `first_day` and `last_day_plus_1` correctly as runtime arguments (`--first_day` and `--last_day_plus_1`) to the Dataproc Serverless job.

---

=== FILE: vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.ksh ===
#! /bin/ksh
# Script generated by software licensed from Ab Initio Software Corporation.
# Use and disclosure are subject to Ab Initio confidentiality and license terms.
export AB_HOME;AB_HOME=${AB_HOME:-/appl/local/abinitio/abinitio-V2-14}
export MPOWERHOME;MPOWERHOME="$AB_HOME"
export PATH
typeset _ab_uname=`uname`
case "$_ab_uname" in
Windows_* )
    PATH="$AB_HOME/bin;$PATH" ;;
CYGWIN_* )
    PATH="`cygpath "$AB_HOME"`/bin:/usr/local/bin:/usr/bin:/bin:$PATH" ;;
* )
    PATH="$AB_HOME/bin:$PATH" ;;
esac
unset ENV
export AB_REPORT;AB_REPORT=${AB_REPORT:-'monitor=60 processes scroll=true'}
export XX_REPORT;XX_REPORT=${XX_REPORT:-'monitor=60 processes scroll=true'}
unset GDE_EXECUTION

# Deployed execution script for graph "BHB_CCM_PROC_WriteContractMapLookup", compiled at Tuesday, October 30, 2007 14:24:31 using GDE version 1.14.16
export AB_JOB;AB_JOB=${AB_JOB_PREFIX:-""}BHB_CCM_PROC_WriteContractMapLookup
# Begin Ab Initio shell utility functions

: ${_ab_uname:=$(uname)}

function __AB_INVOKE_PROJECT
{
  typeset _AB_PROJECT_KSH="$1" ; shift
  typeset _AB_PROJECT_DIR="$1" ; shift
  typeset _AB_DEFINE_OR_EXECUTE="$1" ; shift
  typeset _AB_START_OR_END="$1" ; shift
  if [ $# -gt 0 ] ; then
    . "$_AB_PROJECT_KSH" "$_AB_PROJECT_DIR" "$_AB_DEFINE_OR_EXECUTE" "$_AB_START_OR_END"  "$@"
  else
    . "$_AB_PROJECT_KSH" "$_AB_PROJECT_DIR" "$_AB_DEFINE_OR_EXECUTE" "$_AB_START_OR_END" 
  fi;
}

function __AB_DOTIT
{
  if [ $# -gt 0 ] ; then
    .  "$@"
  fi
}

function __AB_QUOTEIT {
  typeset queue q qq qed lotsaqs s trail
  q="'"
  qq='"'
  if [ X"$1" = X"" ] ; then
    print $q$q
    return
  fi
  queue=${1%$q}
  if [ X"$queue" != X"$1" ] ; then
    trail="${qq}${q}${qq}" 
  else 
    trail=""
  fi
  lotsaqs=${q}${qq}${q}${qq}${q}
  oldIFS="$IFS"
  IFS=$q
  set -- $queue
  IFS="$oldIFS"
  print -rn "$q$1"
  shift
  for s; do
    print -rn "$lotsaqs$s"
  done
  print -r $q$trail
}

function __AB_dirname {
    case $_ab_uname in
    Windows_* | CYGWIN_* )
        typeset d='' p="$1"
        # Strip drive letter colon, if present, and put it into d.
        case $p in
        [A-Za-z]:* )
            d=${p%%:*}:
            p=${p#??}
            ;;
        esac
        # Remove trailing separators, though not the last character in the
        # pathname.
        while : true; do
            case $p in
            ?*[/\\] )
                p=${p%[/\\]} ;;
            * )
                break ;;
            esac
        done
        if [[ "$p" = ?*[/\\]* ]] ; then
            print -r -- "$d${p%[/\\]*}"
        elif [[ "$p" = [/\\]* ]] ; then
            print "$d/"
        else
            print "$d." 
        fi
        ;;
    * ) # Unix
        typeset p="$1"
        # Remove trailing separators, though not the last character in the
        # pathname.
        while : true; do
            case $p in
            ?*/ )
                p="${p%/}" ;;
            * )
                break ;;
            esac
        done
        case $p in
        ?*/* )
            print -r -- "${p%/*}" ;;
        /* )
            print / ;;
        * )
            print . ;;
        esac
        ;;
    esac
}

function __AB_concat_pathname {
    case $_ab_uname in
    Windows_* | CYGWIN_* )
        # Does not handle all cases of concatenating partially absolute
        # pathnames, those with only one of a drive letter or an initial
        # separator.
        case $2 in
        [/\\]* | [A-Za-z]:* )
            print -r -- "$2"
            ;;
        * )
            case $1 in
            # Assume that empty string means ".".  Avoid adding a
            # redundant separator.
            '' | *[/\\] )
                print -r -- "$1$2" ;;
            * )
                print -r -- "$1/$2" ;;
            esac
            ;;
        esac
        ;;
    * ) # Unix
        case $2 in
        /* )
            print -r -- "$2"
            ;;
        * )
            case $1 in
            # Assume that empty string means ".".  Avoid adding a
            # redundant separator.
            '' | */ )
                print -r -- "$1$2" ;;
            * )
                print -r -- "$1/$2" ;;
            esac
            ;;
        esac
        ;;
    esac
}

function __AB_COND {
if [ X"$1" = X0  -o X"$1" = Xfalse -o X"$1" = XFalse -o X"$1" = XF -o X"$1" = Xf ] ; then
  print "0"
else
  print "1"
fi
}

# End Ab Initio shell utility functions

if [ X"${PROJECT_DIR:-}" = X"" ]; then
  # Compute the script directory from $0
  __ab_arg0="$0"
  # Expand symlinks.
  while [ -L "$__ab_arg0" ]
  do
    if [ ! -f "$__ab_arg0" ]; then
      print -r \
"Internal error: '$0' is a symlink and some problem occurred expanding
it.  Please define the environment variable PROJECT_DIR to be the project
base directory before invoking this script."
      exit 1
    fi
    __ab_ls_output="$(/bin/ls -ld "$__ab_arg0")"
    __ab_target_pathname="${__ab_ls_output#*-> }"
    __ab_arg0="$(__AB_concat_pathname "$(__AB_dirname "$__ab_arg0")" "$__ab_target_pathname")"
  done
  
  __ab_script_dir="$(__AB_dirname "$__ab_arg0")"
fi

export AB_GRAPH_NAME;AB_GRAPH_NAME=BHB_CCM_PROC_WriteContractMapLookup

_AB_PROXY_DIR=BHB_CCM_PROC_WriteContractMapLookup-ProxyDir-$$
rm -rf "${_AB_PROXY_DIR}"
mkdir "${_AB_PROXY_DIR}"
print -r -- "" > "${_AB_PROXY_DIR}"'/GDE-Parameters'
function __AB_CLEANUP_PROXY_FILES
{
   rm -rf "${_AB_PROXY_DIR}"
   rm -rf "${AB_EXTERNAL_PROXY_DIR}"
   return
}
trap '__AB_CLEANUP_PROXY_FILES' EXIT
# Work around pdksh bug: the EXIT handler is not executed upon a signal.
trap '_AB_status=$?; __AB_CLEANUP_PROXY_FILES; exit $_AB_status' HUP INT QUIT TERM
# Project Parameters:
export PROJECT_DIR;PROJECT_DIR=${PROJECT_DIR:-"$(cd ${__ab_script_dir}/..; pwd)"}
case "$_ab_uname" in
CYGWIN_* )
   PROJECT_DIR="$(cygpath -m "$PROJECT_DIR")"
esac
typeset _AB_SAVED_PROJECT_DIR
_AB_SAVED_PROJECT_DIR="${PROJECT_DIR}"
_REPOSIT_TRACKING=$(m_env -get AB_GRAPH_SCRIPT_REPOSIT_TRACKING)
if [ X"${_REPOSIT_TRACKING}" = Xtrue -o \( \( X"${_REPOSIT_TRACKING}" = Xdefault -o X"${_REPOSIT_TRACKING}" = "X<unset>" \) -a X"${1}" = X-reposit-tracking \) ]; then
   if [ X"${1}" = X-reposit-tracking ]; then
      shift
   fi
   _AB_PROJECT_NAME=$(air sandbox find "${PROJECT_DIR}" -project)
   if [ $? != 0 ]; then
      print -r -- 'Error: cannot determine path to project in EME Datastore; exiting'
      exit 1
   fi
   export AB_MODIFIED_AIR_JOB_FILENAME;   AB_MODIFIED_AIR_JOB_FILENAME="${_AB_PROXY_DIR}"'/Air-Job-Name'
   if ( grep rec-mode ${AB_HOME}/bin/run-and-reposit > /dev/null ) ; then
      if [ $# -gt 0 ]; then
         AB_GRAPH_SCRIPT_REPOSIT_TRACKING=false ${AB_HOME}/bin/run-and-reposit "${_AB_PROJECT_NAME}"'/mp/BHB_CCM_PROC_WriteContractMapLookup.mp' "${_AB_PROJECT_NAME}" _abort "$0" "$@"
      else
         AB_GRAPH_SCRIPT_REPOSIT_TRACKING=false ${AB_HOME}/bin/run-and-reposit "${_AB_PROJECT_NAME}"'/mp/BHB_CCM_PROC_WriteContractMapLookup.mp' "${_AB_PROJECT_NAME}" _abort "$0"
      fi
   else
      if [ $# -gt 0 ]; then
         AB_GRAPH_SCRIPT_REPOSIT_TRACKING=false ${AB_HOME}/bin/run-and-reposit "${_AB_PROJECT_NAME}"'/mp/BHB_CCM_PROC_WriteContractMapLookup.mp' "${_AB_PROJECT_NAME}" "$0" "$@"
      else
         AB_GRAPH_SCRIPT_REPOSIT_TRACKING=false ${AB_HOME}/bin/run-and-reposit "${_AB_PROJECT_NAME}"'/mp/BHB_CCM_PROC_WriteContractMapLookup.mp' "${_AB_PROJECT_NAME}" "$0"
      fi
   fi
   exit $?
fi
if [ $# -gt 0 ]; then
   __AB_INVOKE_PROJECT "${_AB_SAVED_PROJECT_DIR}"/.project.ksh "${_AB_SAVED_PROJECT_DIR}" execute start "$@"
else
   __AB_INVOKE_PROJECT "${_AB_SAVED_PROJECT_DIR}"/.project.ksh "${_AB_SAVED_PROJECT_DIR}" execute start
fi

if [ $# -gt 0 -a X"$1" = X"-help" ]; then
exit 1
fi
export comment_db1;comment_db1='####################################'
export comment_db2;comment_db2='# BHB Environment Settings'
export comment_db3;comment_db3='# (Database Connections)'
export comment_db4;comment_db4='####################################'
export DB_TNS_NAME_DWH;DB_TNS_NAME_DWH=${DB_TNS_NAME_DWH:-$DB_TNS_NAME_DWH}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_TNS_NAME_DWH of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_USER_DWH;DB_USER_DWH=${DB_USER_DWH:-$DB_USER_DWH}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_USER_DWH of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_PASSWD_DWH;DB_PASSWD_DWH=${DB_PASSWD_DWH:-$DB_PASSWD_DWH}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_PASSWD_DWH of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_DB_VERSION_DWH;DB_DB_VERSION_DWH=${DB_DB_VERSION_DWH:-$DB_DB_VERSION_DWH}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_DB_VERSION_DWH of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_CLIENT_VERSION_DWH;DB_CLIENT_VERSION_DWH=${DB_CLIENT_VERSION_DWH:-$DB_CLIENT_VERSION_DWH}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_CLIENT_VERSION_DWH of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_DB_HOME_DWH;DB_DB_HOME_DWH=${DB_DB_HOME_DWH:-$DB_DB_HOME_DWH}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_DB_HOME_DWH of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_TNS_NAME_CRS;DB_TNS_NAME_CRS=${DB_TNS_NAME_CRS:-$DB_TNS_NAME_CRS}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_TNS_NAME_CRS of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_USER_CRS;DB_USER_CRS=${DB_USER_CRS:-$DB_USER_CRS}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_USER_CRS of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_PASSWD_CRS;DB_PASSWD_CRS=${DB_PASSWD_CRS:-$DB_PASSWD_CRS}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_PASSWD_CRS of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_TNS_NAME_SGM;DB_TNS_NAME_SGM=${DB_TNS_NAME_SGM:-$DB_TNS_NAME_SGM}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_TNS_NAME_SGM of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_USER_SGM;DB_USER_SGM=${DB_USER_SGM:-$DB_USER_SGM}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_USER_SGM of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_PASSWD_SGM;DB_PASSWD_SGM=${DB_PASSWD_SGM:-$DB_PASSWD_SGM}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_PASSWD_SGM of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_TNS_NAME_CADS;DB_TNS_NAME_CADS=${DB_TNS_NAME_CADS:-$DB_TNS_NAME_CADS}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_TNS_NAME_CADS of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_USER_CADS;DB_USER_CADS=${DB_USER_CADS:-$DB_USER_CADS}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_USER_CADS of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_PASSWD_CADS;DB_PASSWD_CADS=${DB_PASSWD_CADS:-$DB_PASSWD_CADS}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_PASSWD_CADS of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_TNS_NAME_CACM;DB_TNS_NAME_CACM=${DB_TNS_NAME_CACM:-$DB_TNS_NAME_CACM}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_TNS_NAME_CACM of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_USER_CACM;DB_USER_CACM=${DB_USER_CACM:-$DB_USER_CACM}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_USER_CACM of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export DB_PASSWD_CACM;DB_PASSWD_CACM=${DB_PASSWD_CACM:-$DB_PASSWD_CACM}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_PASSWD_CACM of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export comment_env1;comment_env1='####################################'
export comment_env2;comment_env2='# BHB Environment Settings'
export comment_env3;comment_env3='# (Framework Parameter)'
export comment_env4;comment_env4='####################################'
export BHB_Projektverzeichnis;BHB_Projektverzeichnis=${BHB_Projektverzeichnis:-$BHB_Projektverzeichnis}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Projektverzeichnis of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export BHB_Graph;BHB_Graph=${BHB_Graph:-$BHB_Graph}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Graph of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export BHB_Prozesstyp;BHB_Prozesstyp=${BHB_Prozesstyp:-$BHB_Prozesstyp}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Prozesstyp of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export BHB_Eintragsnr;BHB_Eintragsnr=${BHB_Eintragsnr:-$BHB_Eintragsnr}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Eintragsnr of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export BHB_Quellverzeichnis;BHB_Quellverzeichnis=${BHB_Quellverzeichnis:-$BHB_Quellverzeichnis}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Quellverzeichnis of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export BHB_Zielverzeichnis;BHB_Zielverzeichnis=${BHB_Zielverzeichnis:-$BHB_Zielverzeichnis}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Zielverzeichnis of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export BHB_Dateimaske;BHB_Dateimaske=${BHB_Dateimaske:-$BHB_Dateimaske}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Dateimaske of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export BHB_Kopfdatensatzkennung;BHB_Kopfdatensatzkennung=${BHB_Kopfdatensatzkennung:-$BHB_Kopfdatensatzkennung}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Kopfdatensatzkennung of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export BHB_Nutzdatensatzkennung;BHB_Nutzdatensatzkennung=${BHB_Nutzdatensatzkennung:-$BHB_Nutzdatensatzkennung}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Nutzdatensatzkennung of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export BHB_Endedatensatzkennung;BHB_Endedatensatzkennung=${BHB_Endedatensatzkennung:-$BHB_Endedatensatzkennung}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Endedatensatzkennung of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export BHB_Dateiname;BHB_Dateiname=${BHB_Dateiname:-$BHB_Dateiname}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Dateiname of BHB_CCM_PROC_WriteContractMapLookup', interpretation 'shell'
   exit $mpjret
fi
export comment_loc_1;comment_loc_1='####################################'
export comment_loc_2;comment_loc_2='# BHB Local Settings'
export comment_loc_3;comment_loc_3='# (Special Parameter)'
export comment_loc_4;comment_loc_4='####################################'
export BHB_CCM_PROC_TargetObjectName;BHB_CCM_PROC_TargetObjectName=ContractMapLookup.txt
export BHB_CCM_PROC_FirstDay;BHB_CCM_PROC_FirstDay=${BHB_CCM_PROC_FirstDay:-20050217}
export BHB_CCM_PROC_LastDayPlus1;BHB_CCM_PROC_LastDayPlus1=${BHB_CCM_PROC_LastDayPlus1:-20050218}
. ./${_AB_PROXY_DIR}/GDE-Parameters

#+Script Start+  ==================== Edits in this section are preserved.
#+End Script Start+  ====================
if [ -f "$AB_HOME/bin/ab_catalog_functions.ksh" ]; then . ab_catalog_functions.ksh; fi
if [ "${AB_MODIFIED_AIR_JOB_FILENAME}" != "" ] && [ "${AB_ORIGINAL_AIR_JOB}" != "" ] && [ "${AB_ORIGINAL_AIR_JOB}" != "${AB_AIR_JOB}" ]; then
   air rm -r -f "${AB_AIR_JOB}"
   if [ $? != 0 ]; then
      exit 1
   fi
   air mv "${AB_ORIGINAL_AIR_JOB}" "${AB_AIR_JOB}"
   if [ $? != 0 ]; then
      exit 1
   fi
   print -r -- "${AB_AIR_JOB}" > "${AB_MODIFIED_AIR_JOB_FILENAME}"
fi
mv "${_AB_PROXY_DIR}" "${AB_JOB}"'-BHB_CCM_PROC_WriteContractMapLookup-ProxyDir'
_AB_PROXY_DIR="${AB_JOB}"'-BHB_CCM_PROC_WriteContractMapLookup-ProxyDir'
print -r -- '/* DML Generated for SQL: SELECT * FROM DWH$TA_L_MAP_VT_CARM_DWH
 * On: Fri Jun 17 11:07:57 2005

 */
record
  decimal("\001", maximum_length=13) vertrags_id; /* NUMBER(10) NOT NULL*/
  decimal("\001", maximum_length=19) dwh_vertrag_id = NULL(""); /* NUMBER(16)*/
  string(1) newline = "\n";
end' > "${_AB_PROXY_DIR}"'/DWH_TA_L_MAP_VT_CARM_DWH-2.dml'
print -r -- 'out::reformat(in) =
begin
  out.* :: in.*;
end;' > "${_AB_PROXY_DIR}"'/Extract_Contract_map_attributes-3.xfr'
print -r -- 'record
  string(";") TARGET_OBJECT_NAME;
  datetime("YYYYMMDD")(";") FIRST_DAY;
  datetime("YYYYMMDD")(";") LAST_DAY_PLUS_1;
  string("\n") newline;
end;' > "${_AB_PROXY_DIR}"'/Reformat-5.dml'
print -r -- '/*Reformat operation*/
out::reformat(in) =
begin
  out.* :: in.*;
end;' > "${_AB_PROXY_DIR}"'/Reformat-6.xfr'
print -r -- 'record
  string("\001", maximum_length=50) TARGET_OBJECT_NAME;
  datetime("YYYYMMDD")("\001") FIRST_DAY;
  datetime("YYYYMMDD")("\001") LAST_DAY_PLUS_1;
  string(1) newline;
end;' > "${_AB_PROXY_DIR}"'/Join_with_DB-7.dml'
print -r -- 'type query_result_type = 
record
  decimal("\001", maximum_length=102)  result = NULL("");
end /* Generated type from select statement*/;


/*This type may be optionally defined.
// Compute fields for where clause
type key_type = NULL_TYPE;

This type may be optionally defined.
// Computed data for insert statement
type insert_type = NULL_TYPE;

Database lookup transform*/
out::join_with_db(in, query_result) =
begin
  out.RESULT :: query_result.result;
end;' > "${_AB_PROXY_DIR}"'/Join_with_DB-9.xfr'
print -r -- 'record
  integer(8) RESULT;
end;' > "${_AB_PROXY_DIR}"'/Join_with_DB-11.dml'

mp job ${AB_JOB}

# Layouts:
m_db_layout layout1 ${BHB_DB}/DWH_BHB.dbc -serial

# Record Formats (Metadata):
mp metadata metadata1 -file "${_AB_PROXY_DIR}"'/DWH_TA_L_MAP_VT_CARM_DWH-2.dml'
mp metadata metadata2 -file "$CCM_PROC_ContractMapLookupDML"
mp metadata metadata3 -file "${_AB_PROXY_DIR}"'/Reformat-5.dml'
mp metadata metadata4 -file "${_AB_PROXY_DIR}"'/Join_with_DB-7.dml'
mp metadata metadata5 -file "${_AB_PROXY_DIR}"'/Join_with_DB-11.dml'

export AB_CATALOG;AB_CATALOG=${AB_CATALOG:-"${XX_CATALOG}"}
# Catalog Usage: Creating temporary catalog using lookup files only
m_rmcatalog -catalog GDE-BHB_CCM_PROC_WriteContractMapLookup-${AB_JOB}.cat > /dev/null 2>&1
m_mkcatalog -catalog GDE-BHB_CCM_PROC_WriteContractMapLookup-${AB_JOB}.cat
SAVED_CATALOG="${AB_CATALOG}"
export AB_CATALOG;AB_CATALOG='GDE-BHB_CCM_PROC_WriteContractMapLookup-'"${AB_JOB}"'.cat'
export XX_CATALOG;XX_CATALOG="${AB_CATALOG}"

# Files:
mp ofile Contract_Map_Lookup_File "$CCM_PROC_ContractMapLookupFilename"

# Components in phase 0:
mp itable DWH_TA_L_MAP_VT_CARM_DWH__table_ "$BHB_DB"'/DWH_BHB.dbc' -table 'DWH$TA_L_MAP_VT_CARM_DWH' -interface api -field_type_preference delimited -layout Contract_Map_Lookup_File
mp reformat-transform Extract_Contract_map_attributes -limit 0 -ramp 0.0 -ramp 0.0 -layout Contract_Map_Lookup_File
mp add-port Extract_Contract_map_attributes.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Extract_Contract_map_attributes-3.xfr'
mp local-sort Sort '{vertrags_id}' -max-core 100663296 -layout Contract_Map_Lookup_File
mp filter Update_Loading_Timestamps.Run_Program echo "${BHB_CCM_PROC_TargetObjectName};${BHB_CCM_PROC_FirstDay};${BHB_CCM_PROC_LastDayPlus1};" -layout layout1
mp reformat-transform Update_Loading_Timestamps.Reformat -limit 0 -ramp 0.0 -ramp 0.0 -layout layout1
mp add-port Update_Loading_Timestamps.Reformat.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat-6.xfr'
mp db-lookup Update_Loading_Timestamps.Join_with_DB "${BHB_DB}"'/DWH_BHB.dbc' 'execute :result = DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio(:TARGET_OBJECT_NAME, :FIRST_DAY, :LAST_DAY_PLUS_1)' "${_AB_PROXY_DIR}"'/Join_with_DB-9.xfr' ~null -match_required -maximum_matches -1 -commit_number 1 -limit 0 -ramp 0.0 -ramp 0.0 -fixed_size_dml -generate_dml_with_nulls -select -layout layout1
mp broadcast Update_Loading_Timestamps.Trash -layout layout1

# Flows for Entire Graph:
mp straight-flow Flow_2 DWH_TA_L_MAP_VT_CARM_DWH__table_.read Extract_Contract_map_attributes.in -metadata metadata1
mp straight-flow Flow_1 Extract_Contract_map_attributes.out.out0 Sort.in -metadata metadata2
mp straight-flow Flow_3 Sort.out Contract_Map_Lookup_File.write -metadata metadata2
mp straight-flow Update_Loading_Timestamps.Flow_1 Update_Loading_Timestamps.Run_Program.out Update_Loading_Timestamps.Reformat.in -metadata metadata3
mp straight-flow Update_Loading_Timestamps.Flow_3 Update_Loading_Timestamps.Reformat.out.out0 Update_Loading_Timestamps.Join_with_DB.in -metadata metadata4
mp straight-flow Update_Loading_Timestamps.Flow_2 Update_Loading_Timestamps.Join_with_DB.out Update_Loading_Timestamps.Trash.in -metadata metadata5

unset AB_TRACKING_GRAPH_THUMBPRINT
unset AB_COMM_WAIT
mp run
mpjret=$?
unset AB_COMM_WAIT
unset AB_TRACKING_GRAPH_THUMBPRINT
mp reset
m_rmcatalog > /dev/null 2>&1
export XX_CATALOG;XX_CATALOG="${SAVED_CATALOG}"
export AB_CATALOG;AB_CATALOG="${SAVED_CATALOG}"

#+Script End+  ==================== Edits in this section are preserved.
#+End Script End+  ====================
# Project Script end
if [ $# -gt 0 ]; then
   __AB_INVOKE_PROJECT "${_AB_SAVED_PROJECT_DIR}"/.project.ksh "${_AB_SAVED_PROJECT_DIR}" execute end "$@"
else
   __AB_INVOKE_PROJECT "${_AB_SAVED_PROJECT_DIR}"/.project.ksh "${_AB_SAVED_PROJECT_DIR}" execute end
fi

exit $mpjret


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script executes an Ab Initio graph that extracts data to a flat file and invokes a database stored procedure, which requires Python for file orchestration, database client connectivity, and control flow.

EVIDENCE
- Business logic found: KSH custom logic / graph compilation. It extracts data from the Oracle table `DWH$TA_L_MAP_VT_CARM_DWH`, sorts it by `vertrags_id`, writes it to a file specified by `$CCM_PROC_ContractMapLookupFilename`, and calls the stored procedure `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio`.
- AWK: none
- SQL-expressible: No, because it involves writing to a local/shared delimited flat file whose target path is dynamically parameterized ($CCM_PROC_ContractMapLookupFilename), which is outside standard SQL capabilities.
- Non-SQL side effects: Creates temporary proxy directories, writes DML/XFR files, generates an output file on the filesystem, and orchestrates an Ab Initio graph execution.
- Against this verdict: If the target architecture replaces all files with BigQuery tables and we ignore the Ab Initio orchestration, the core data transform could be written as a BQ SQL query (extract and sort), but the file-generation requirements and stored procedure call make Python the safer choice.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The script executes an Ab Initio graph named `BHB_CCM_PROC_WriteContractMapLookup`. It reads contract mapping data from the Oracle table `DWH$TA_L_MAP_VT_CARM_DWH`, formats and sorts it by `vertrags_id`, and writes it to a file. Additionally, it calls a database stored procedure (`DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio`) to log or update the data loading timestamps for the target object.

2. INVOCATION CONTEXT
   - Who calls this script: Typically invoked by a UC4 job (the exact name is not provided in the source text, so we mark it as unknown).
   - UC4 native includes: None referenced in the provided text.
   - Environment files sourced:
     - `.project.ksh` via the `__AB_INVOKE_PROJECT` function.
       # REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown; do not guess their names or values
     - `ab_catalog_functions.ksh` if it exists in `$AB_HOME/bin`.
       # REVIEW-STRUCT: environment file [ab_catalog_functions.ksh] not supplied — variables it sets are unknown; do not guess their names or values
     - `. ./GDE-Parameters` (generated dynamically within the script's proxy directory).

3. PARAMETERS / INPUTS
   The script uses several parameters declared as exports:
   - Positional arguments: `$1` may be `-reposit-tracking` or `-help`.
   - DB Connection parameters (from KSH DECLARED ENVIRONMENT PARAMETERS section):
     - `DB_TNS_NAME_DWH`, `DB_USER_DWH`, `DB_PASSWD_DWH`, `DB_DB_VERSION_DWH`, `DB_CLIENT_VERSION_DWH`, `DB_DB_HOME_DWH` (Oracle DWH connection credentials - used for database connectivity).
       # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying
     - `DB_TNS_NAME_CRS`, `DB_USER_CRS`, `DB_PASSWD_CRS` (Unused in the script, informational only).
     - `DB_TNS_NAME_SGM`, `DB_USER_SGM`, `DB_PASSWD_SGM` (Unused in the script, informational only).
     - `DB_TNS_NAME_CADS`, `DB_USER_CADS`, `DB_PASSWD_CADS` (Unused in the script, informational only).
     - `DB_TNS_NAME_CACM`, `DB_USER_CACM`, `DB_PASSWD_CACM` (Unused in the script, informational only).
   - Framework Parameters (Informational only, unused in the core logic):
     - `BHB_Projektverzeichnis`, `BHB_Graph`, `BHB_Prozesstyp`, `BHB_Eintragsnr`, `BHB_Quellverzeichnis`, `BHB_Zielverzeichnis`, `BHB_Dateimaske`, `BHB_Kopfdatensatzkennung`, `BHB_Nutzdatensatzkennung`, `BHB_Endedatensatzkennung`, `BHB_Dateiname`
   - Local Settings:
     - `BHB_CCM_PROC_TargetObjectName` (Default: `ContractMapLookup.txt`) - Used in stored procedure call.
     - `BHB_CCM_PROC_FirstDay` (Default: `20050217`) - Used in stored procedure call.
     - `BHB_CCM_PROC_LastDayPlus1` (Default: `20050218`) - Used in stored procedure call.
   - File path parameters:
     - `CCM_PROC_ContractMapLookupDML` - Metadata definition path (unused in Python conversion as schemas are embedded).
     - `CCM_PROC_ContractMapLookupFilename` - Target output file path.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   The script invokes Ab Initio command-line utilities (`m_env`, `air`, `mp`, `m_db_layout`, `m_rmcatalog`, `m_mkcatalog`).
   Since we are converting the script's *business logic* to Python, these proprietary tools will not be invoked. Instead, their logic is "resolved" by replacing them with native Python functions (e.g. standard file I/O, Python db-clients, standard library manipulation).
   This qualifies as a **RESOLVABLE LAUNCHER** pattern because the underlying Ab Initio graph logic compiles down to a clean, traceable database query and a stored procedure call.
   - Target database: Oracle (implied by Oracle-specific stored procedure block call syntax and schema tables).
     # REVIEW: target database platform not specified; DB-client library choice below is provisional

5. EMBEDDED SQL
   The script has inline references to database objects:
   - Source table: `DWH$TA_L_MAP_VT_CARM_DWH`
     - Fields extracted (derived from `DWH_TA_L_MAP_VT_CARM_DWH-2.dml`):
       - `vertrags_id` (NUMBER(10) NOT NULL)
       - `dwh_vertrag_id` (NUMBER(16) NULL)
     - Core Query:
       ```sql
       SELECT vertrags_id, dwh_vertrag_id FROM DWH$TA_L_MAP_VT_CARM_DWH
       ```
   - DB Lookup (Stored Procedure call):
     ```sql
     execute :result = DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio(:TARGET_OBJECT_NAME, :FIRST_DAY, :LAST_DAY_PLUS_1)
     ```
     - Statement type: PL-SQL Stored Procedure Execution
     - Parameters mapped:
       - `:TARGET_OBJECT_NAME` -> `BHB_CCM_PROC_TargetObjectName`
       - `:FIRST_DAY` -> `BHB_CCM_PROC_FirstDay`
       - `:LAST_DAY_PLUS_1` -> `BHB_CCM_PROC_LastDayPlus1`

6. CONTROL FLOW
   1. **Initialization**: Read environment variables, default variables, and command-line arguments.
   2. **Project Lifecycle Hook (Start)**: Source `.project.ksh` with argument `execute start`.
   3. **Database Extraction**: Query the `DWH$TA_L_MAP_VT_CARM_DWH` table using the credentials supplied by `DB_USER_DWH`, `DB_PASSWD_DWH`, and `DB_TNS_NAME_DWH`.
   4. **Transform and Sort**:
      - Fetch all records (containing `vertrags_id` and `dwh_vertrag_id`).
      - Sort the records by `vertrags_id` ascending.
   5. **File Generation**: Write the sorted records to the delimited flat file path specified by `$CCM_PROC_ContractMapLookupFilename` using `\001` (SOH) or the specified character as the field delimiter and `\n` as the line terminator.
   6. **Timestamp Update**: Execute the database stored procedure `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` passing target object, start day, and last day plus 1.
   7. **Project Lifecycle Hook (End)**: Source `.project.ksh` with argument `execute end`.
   8. **Cleanup**: Handle any exceptions and ensure database connections are closed.

7. ERROR HANDLING & EXIT CODES
   - How does the script detect failure? It checks exit codes of parameter evaluations and graph runs (`mpjret=$?`), exiting immediately with a non-zero code on failure.
   - Python Mapping: Python exceptions will be handled using a standard `try...except...finally` block. A connection failure, missing environment variable, or failed database operation will raise an exception, print to `sys.stderr`, and trigger a clean exit with a non-zero exit code (e.g., `sys.exit(1)`).
   - Trap handlers are mapped to Python `finally` blocks for clean closure of database sessions.

8. OUTPUTS / SIDE EFFECTS
   - Writes to output file: `$CCM_PROC_ContractMapLookupFilename`
   - Executes stored procedure `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` (modifies state in Oracle DB).

9. BUSINESS SUMMARY
   - Reads contract mapping data from database table `DWH$TA_L_MAP_VT_CARM_DWH`.
   - Extracts `vertrags_id` and `dwh_vertrag_id` attributes.
   - Sorts the extracted records by `vertrags_id`.
   - Writes the formatted and sorted contract map to a localized/configured delimited flat file.
   - Updates database load tracking metadata by calling `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` with parameter boundaries (`ContractMapLookup.txt`, start date, end date).

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
import os
import sys
import oracledb  # Provisional choice based on Oracle SQL dialect indicators

# Step 1: Load environment parameters and establish defaults
# REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying
DB_USER = os.environ.get("DB_USER_DWH")
DB_PASSWD = os.environ.get("DB_PASSWD_DWH")
DB_TNS_NAME = os.environ.get("DB_TNS_NAME_DWH")

CCM_PROC_ContractMapLookupFilename = os.environ.get("CCM_PROC_ContractMapLookupFilename")

BHB_CCM_PROC_TargetObjectName = os.environ.get("BHB_CCM_PROC_TargetObjectName", "ContractMapLookup.txt")
BHB_CCM_PROC_FirstDay = os.environ.get("BHB_CCM_PROC_FirstDay", "20050217")
BHB_CCM_PROC_LastDayPlus1 = os.environ.get("BHB_CCM_PROC_LastDayPlus1", "20050218")

# Validate required variables
if not all([DB_USER, DB_PASSWD, DB_TNS_NAME, CCM_PROC_ContractMapLookupFilename]):
    print("Error: Missing required environment variables.", file=sys.stderr)
    sys.exit(1)

# Step 2: Source legacy project lifecycle logic (Simulated or Placeholder)
# REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown; do not guess their names or values
# Note: Legacy script executed `.project.ksh <dir> execute start`. If these hooks are required, they should be invoked here.

connection = None
cursor = None

try:
    # Step 3: Establish connection to database
    # REVIEW: target database platform not specified; DB-client library choice below is provisional
    connection = oracledb.connect(user=DB_USER, password=DB_PASSWD, dsn=DB_TNS_NAME)
    cursor = connection.cursor()

    # Step 4: Extract contract mapping attributes
    query = "SELECT vertrags_id, dwh_vertrag_id FROM DWH$TA_L_MAP_VT_CARM_DWH"
    cursor.execute(query)
    records = cursor.fetchall()

    # Step 5: Format and Sort data by vertrags_id
    # vertrags_id is the first element in each row (index 0)
    # Sort order is ascending. None values are sorted last or converted to empty strings.
    sorted_records = sorted(records, key=lambda x: (x[0] is None, x[0]))

    # Step 6: Write output to delimited flat file
    # Format maps to the DML spec: decimal("\001") vertrags_id, decimal("\001") dwh_vertrag_id, and newline = "\n"
    with open(CCM_PROC_ContractMapLookupFilename, 'w', encoding='utf-8') as outfile:
        for vert_id, dwh_vert_id in sorted_records:
            vert_id_str = str(int(vert_id)) if vert_id is not None else ""
            dwh_vert_id_str = str(int(dwh_vert_id)) if dwh_vert_id is not None else ""
            outfile.write(f"{vert_id_str}\x01{dwh_vert_id_str}\n")

    # Step 7: Update Loading Timestamps via Stored Procedure
    # Matches: execute :result = DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio(:TARGET_OBJECT_NAME, :FIRST_DAY, :LAST_DAY_PLUS_1)
    result_var = cursor.var(oracledb.NUMBER)
    cursor.callproc("DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio", [
        BHB_CCM_PROC_TargetObjectName,
        BHB_CCM_PROC_FirstDay,
        BHB_CCM_PROC_LastDayPlus1,
        result_var
    ])
    
    # Optional: Log stored procedure result
    # print(f"Stored Procedure executed. Result: {result_var.getvalue()}")
    connection.commit()

except Exception as e:
    print(f"Error executing python conversion task: {str(e)}", file=sys.stderr)
    if connection:
        connection.rollback()
    sys.exit(1)

finally:
    # Step 8: Close database resources
    if cursor:
        cursor.close()
    if connection:
        connection.close()
    
    # Step 9: Source legacy project lifecycle termination (Simulated or Placeholder)
    # Note: Legacy script executed `.project.ksh <dir> execute end`
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.ksh` | `Retired` | Retired per reviewer feedback to avoid structural redundancy. This KornShell script is a thin wrapper executing the Ab Initio graph. The graph (`.mp` file) is converted to a canonical PySpark script in its own design pass, and the Airflow DAG will execute that PySpark script directly via `DataprocSubmitJobOperator`, rendering this launcher script obsolete. |

### Job dependencies
- **Upstream**:
  - Shared Files: `vobs/dw_source/istools/seu/template` — already migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/852).
- **Downstream**:
  - `DW.CCM_PROC_JP` — not yet migrated. Since the downstream consumer does not yet exist, cross-job wiring cannot be fully finalized (flagged under Risks & Manual Steps).

### Execution order
The target orchestration sequence must map and preserve the legacy execution order:
1. **Legacy Step 1 (UC4 Orchestration)**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/CCM_PROC/PRODUKTION/DW.CCM_PROC_JP/DW.CCM_WRITE_CONTRACTMAPLOOKUP.xml` -> Converted to a Cloud Composer (Airflow) DAG.
2. **Legacy Step 2 (KSH Launcher)**: `vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.ksh` -> **Retired** (this pass).
3. **Legacy Step 3 (Ab Initio Graph)**: `vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.mp` -> Migrated to a PySpark script and executed directly by the Cloud Composer DAG via `DataprocSubmitJobOperator`.

### Scheduling
- **Linkage**: This job is not directly triggered by any standalone scheduler; it executes inside scheduled parent jobs as an included/shared module. In BigQuery/Cloud Composer, it must remain a callable pipeline task within the parent DAG and inherit its schedule.

### Schedule & variables
- **Schedule**: Inherited from the parent scheduled workflows (no standalone schedule).
- **Scheduler-set variables**: No direct scheduler-set variables are defined for this specific job; environment-wide variables are supplied at the runtime environment/DAG level.

### Lineage
- **Upstream Lineage**:
  - Sourced table: `DWH$TA_L_MAP_VT_CARM_DWH` (Oracle) -> Maps to the BigQuery target table `DWH_TA_L_MAP_VT_CARM_DWH`.
  - Dependent scripts/functions: `AB_CATALOG_FUNCTIONS.KSH` and `ECHO` are human-confirmed as not needed.
- **Downstream Lineage**:
  - Target output: Flat file `$CCM_PROC_ContractMapLookupFilename` (`ContractMapLookup.txt`) -> Replaced by writing to Google Cloud Storage (GCS).

### External system replacements
- **Oracle DB Extraction**: Sourcing from the Oracle database table `DWH$TA_L_MAP_VT_CARM_DWH` is replaced by querying BigQuery.
- **Flat File Writing**: Writing to a local file system is replaced by exporting data to a Google Cloud Storage (GCS) path (e.g. `gs://{GCS_BUCKET}/ccm_proc/ContractMapLookup.txt`).
- **Oracle Stored Procedure**: The Oracle procedure call `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` is replaced by executing a BigQuery stored procedure or updating a load-tracking table in BigQuery.

### Cross-file dependencies
- **Shared configurations**: Sourced from `.dw_global` and `.dw_init` (already migrated).
- **Launcher relationship**: The wrapper script depended on the GDE graph `BHB_CCM_PROC_WriteContractMapLookup.mp`. Because this wrapper is retired, the Airflow DAG will bypass the wrapper and directly coordinate the execution of the migrated PySpark script for the `.mp` graph.

### Target file plan
- **Source File**: `vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.ksh`
- **Target File / Action**: `Retired`
- **Reasoning**: To resolve structural conflicts and redundant implementations of the ETL logic, the wrapper script is retired. The primary execution logic is handled by the migrated PySpark code from the `.mp` file design pass.

### Environment-specific values
The environment values are classified and resolved as follows:

1. **GLOBAL (Environment-wide)**:
   - `GCP_PROJECT`: GCP Project ID. Sourced via Airflow config store: `Variable.get("GCP_PROJECT")`.
   - `GCS_BUCKET`: GCS Bucket for data outputs. Sourced via Airflow config store: `Variable.get("GCS_BUCKET")`.
   - `BQ_DATASET`: Target BigQuery dataset containing the `DWH_TA_L_MAP_VT_CARM_DWH` table. Sourced via Airflow config store: `Variable.get("BQ_DATASET")`.

2. **JOB-SPECIFIC**:
   - `BHB_CCM_PROC_TargetObjectName`: Target tracking object name. Value: `"ContractMapLookup.txt"`.
   - `BHB_CCM_PROC_FirstDay`: Starting timestamp parameter. Value: `"20050217"`.
   - `BHB_CCM_PROC_LastDayPlus1`: Ending timestamp parameter. Value: `"20050218"`.
   - `CCM_PROC_ContractMapLookupFilename`: Path to the generated flat file. Mapped to: `gs://{GCS_BUCKET}/ccm_proc/ContractMapLookup.txt`.

### Risks and manual steps
- **SOURCE: NOT FOUND - AB_CATALOG_FUNCTIONS.KSH - no candidate**: Human-confirmed resolution indicates that this utility file is not needed on the target platform, but its omission should be verified during DAG integration.
- **SOURCE: NOT FOUND - ECHO - no candidate**: Human-confirmed resolution indicates that this utility is not needed.
- **Downstream Orchestration Wiring**: The downstream job `DW.CCM_PROC_JP` is not yet migrated. The final orchestration and sensor/task triggering cannot be verified or finalized until `DW.CCM_PROC_JP` is completed.
- **Airflow DAG Operator Update**: The Airflow DAG that replaces the UC4 job must not use an `EmptyOperator` for this step. Instead, it must be configured with a `DataprocSubmitJobOperator` to submit the PySpark script generated from the `.mp` GDE graph design pass, ensuring the execution flow is preserved.