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


# UC4 to Apache Airflow Migration Design Document

## 1. Overview
This migration design document covers the standalone UC4 UNIX job `DW.CCM_WRITE_CONTRACTMAPLOOKUP`. Based on the extraction, this object runs an Ab Initio graph wrapper script (`BHB_CCM_PROC_WriteContractMapLookup.ksh`) to execute the Ab Initio graph `BHB_CCM_PROC_WriteContractMapLookup.mp`. It processes Contract Map Lookup data within the `CCM_PROC` module. 

Since no parent Job Plan (`JOBP`), Schedule (`JSCH`), or Script trigger (`SCRI`) was supplied in this extraction, this job is treated as an externally triggered, single-task workflow in Airflow with an unknown external orchestration source.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.CCM_WRITE_CONTRACTMAPLOOKUP` | JOBS_UNIX | Active (1) | CCM_PROC: Write Contract Map Lookup (Ab Initio graph) |

---

## 3. Scheduling
* **Calendar Schedule:** No schedule-defining objects (`EVNT_TIME`) are present in this extraction. 
* **Triggering Mechanism:** Externally triggered (source unknown from this extraction alone). No referencing `SCRI` or `JOBP` objects are bundled.
* **Airflow Schedule:** `schedule=None` (triggered manually, externally, or via dataset/API events).

---

## 4. Airflow DAG Properties
Since no parent `JOBP` exists in the extraction, a single-task DAG is generated directly for this standalone job.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_ccm_write_contractmaplookup` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1)* |
| **default_args** | `{'owner': 'DW', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_ccm_write_contractmaplookup` | `DW.CCM_WRITE_CONTRACTMAPLOOKUP` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | **#REVIEW-STRUCT:** Launcher command `&HOME/abinitio/bin/BHB_CCM_PROC_WriteContractMapLookup.ksh` not recognised — confirm target operator/script manually. |

---

## 6. Task Dependency Map
Since this DAG contains only one standalone task, there is no dependency chain to map:

```python
dw_ccm_write_contractmaplookup
```

---

## 7. Sync / Concurrency Analysis
No `sync_rows` (UC4 locks) are declared for this object in the extraction. Concurrency is limited to `max_active_runs=1` at the DAG level to prevent concurrent execution on the same environment.

---

## 8. Error Handling and Retry Strategy
* **Retries:** Standardized to 1 retry with a 5-minute delay based on general best-practice templates.
* **Failure Handling:** Standard Airflow failure propagation. No custom `on_failure_callback` was specified in the extraction.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `DW.CCM_WRITE_CONTRACTMAPLOOKUP` | Standalone JOBS_UNIX Object | Sanitised DAG ID: `dw_ccm_write_contractmaplookup` |
| Host: `\|DWHDWH2P\|HOST` | Target Execution Server | Airflow Connection ID (e.g., `ssh_dwh_host`) |
| Login: `DW.UNIX.ISDWH` | Unix Service Account | SSH connection/execution user context |

---

## 10. Developer Notes
* **#REVIEW-STRUCT: Standalone Job Object:** This extraction contains only a single `JOBS_UNIX` object with no enclosing workflow. We have structured this as a single-task DAG `dw_ccm_write_contractmaplookup`.
* **#REVIEW-STRUCT: Unrecognized Launcher:** The launcher command is `&HOME/abinitio/bin/BHB_CCM_PROC_WriteContractMapLookup.ksh` which was classified as `unrecognized`. It is mapped to an `EmptyOperator` stub. If migrating the Ab Initio graph directly to GCP (e.g., using Dataproc/PySpark), this should be converted to a `DataprocSubmitJobOperator`. Alternatively, if executing the script on-premises or via an existing VM, use a `BashOperator` or `SSHOperator` calling the `.ksh` wrapper.
* **Environment Initialization:** The original UC4 script sourced environment variables using `. $HOME/.dw_init` before launching the wrapper. Ensure that equivalent environment initialization occurs within the target Airflow execution context (e.g., inside the SSH session profile or target container).

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# Placeholder for GCP execution environment configuration if migrating to Cloud:
# GCP_PROJECT_ID = "your-gcp-project-id"
# GCP_REGION = "your-region"

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'DW',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom failure callbacks defined in original UC4 source.

# ── DAG Definition ──────────────────────────────────────────
with DAG(
    dag_id='dw_ccm_write_contractmaplookup',
    default_args=default_args,
    description='CCM_PROC: Write Contract Map Lookup (Ab Initio graph)',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated_uc4', 'ccm_proc'],
) as dag:

    # ── Guard Task ────────────────────────────────────────
    # None required (no Else=Skip self-locks present).

    # ── Sensor Task ───────────────────────────────────────
    # None required (no earliest_start_time constraint).

    # ── Calendar Check Task ───────────────────────────────
    # None required (no calendar constraints specified).

    # ── Task: dw_ccm_write_contractmaplookup ──────────────
    # #REVIEW-STRUCT: Launcher command &HOME/abinitio/bin/BHB_CCM_PROC_WriteContractMapLookup.ksh
    # was not recognized. This is mapped to an EmptyOperator stub. 
    #
    # Developer Action: Convert this task to the appropriate execution operator.
    # If maintaining the legacy script execution via SSH:
    #     from airflow.providers.ssh.operators.ssh import SSHOperator
    #     task = SSHOperator(
    #         task_id='dw_ccm_write_contractmaplookup',
    #         ssh_conn_id='ssh_dwh_host',
    #         command='. $HOME/.dw_init && $HOME/abinitio/bin/BHB_CCM_PROC_WriteContractMapLookup.ksh'
    #     )
    #
    # If migrating to GCP Dataproc (PySpark):
    #     from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
    #     ...
    
    dw_ccm_write_contractmaplookup = EmptyOperator(
        task_id='dw_ccm_write_contractmaplookup',
    )

    # ── Dependencies ─────────────────────────────────────────
    # Trivial workflow containing only the single standalone task.
    dw_ccm_write_contractmaplookup
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/CCM_PROC/PRODUKTION/DW.CCM_PROC_JP/DW.CCM_WRITE_CONTRACTMAPLOOKUP.xml` | `dags/vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/CCM_PROC/PRODUKTION/DW.CCM_PROC_JP/dw_ccm_write_contractmaplookup.py` | Migrates the UC4 Unix Job definition to an Airflow DAG that orchestrates the execution of the associated PySpark job (migrated from the Ab Initio graph). |

***

### Job dependencies
* **Upstream:**
  * Shared Files `vobs/dw_source/istools/seu/template` — Already migrated & merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/852). Sourced environment variables are loaded via target Cloud Composer environment variables or direct DAG configurations.
* **Downstream:**
  * `DW.CCM_PROC_JP` (Job Plan) — Not yet migrated. Once migrated, the target workflow must invoke this DAG (`dw_ccm_write_contractmaplookup`) dynamically. (Flagged under Risks & Manual Steps).

### Execution order
The target Airflow DAG must maintain the following sequence:
1. Environment initialization: Equivalent configurations from `vobs/dw_source/istools/seu/template/.dw_init` must be resolved and active.
2. Core workload: Execute `BHB_CCM_PROC_WriteContractMapLookup.ksh` (which starts `BHB_CCM_PROC_WriteContractMapLookup.mp`). In the target architecture, this maps to triggering the converted PySpark pipeline on Dataproc Serverless.

### Scheduling
* This job is not directly triggered by any scheduler; it runs as an included unit within the parent workflow execution (`DW.CCM_PROC_JP`).
* **Target Mapping:** The DAG is defined with `schedule=None` (manual or externally triggered) to prevent standalone runs and remain a callable module for the downstream orchestrator.

### Schedule & variables
* **Schedule:** Inherited execution from the parent Job Plan (`DW.CCM_PROC_JP`).
* **Variables:**
  * `HOME` / `$HOME`: Handled in the target environment via standard system environment variables or standard Cloud Composer configurations.

### Lineage
* **Upstream Producers:**
  * `.dw_init` script: Configures environment variables.
  * `PACKAGE:DW.UNIX.ISDWH` (Unix Package): Sets login configuration for running on host `dwhdwh2p`.
* **Downstream Consumers:**
  * `vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.ksh` (cross-job hand-off): Initiated by this UC4 job to run the corresponding Ab Initio graph.

### External system replacements
* Host `dwhdwh2p` execution environment and login `DW.UNIX.ISDWH` are replaced by Cloud Composer tasks running with GCP Service Accounts and IAM permissions.

### Cross-file dependencies
* Sourced environment dependency on `.dw_init` is mapped to Composer's system configuration or Airflow Variable lookups.
* Execution dependency on `BHB_CCM_PROC_WriteContractMapLookup.ksh` is replaced by launching the converted PySpark application (representing the `BHB_CCM_PROC_WriteContractMapLookup.mp` graph).

### Target file plan
* **Target File Path:** `dags/vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/CCM_PROC/PRODUKTION/DW.CCM_PROC_JP/dw_ccm_write_contractmaplookup.py`
  * **Language:** Python (Airflow DAG)
  * **Source File:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/CCM_PROC/PRODUKTION/DW.CCM_PROC_JP/DW.CCM_WRITE_CONTRACTMAPLOOKUP.xml`

### Environment-specific values
1. **GLOBAL (Environment-wide):**
   * `HOME` / `$HOME`: Maps to Cloud Composer run-time environment variables. Sourced via `os.environ.get("HOME")` if required, or handled natively by Composer's configuration.
   * Host `dwhdwh2p`: Maps to the global `GCP_PROJECT`, `GCP_REGION`, and GCS buckets where jobs are run.
   * `DW.UNIX.ISDWH` (Login): Replaced by GCP Service Account execution context.
2. **JOB-SPECIFIC:**
   * Script launcher path `$HOME/abinitio/bin/BHB_CCM_PROC_WriteContractMapLookup.ksh`: Replaced by a job-specific PySpark path on GCS (e.g., `gs://{GCS_BUCKET}/pyspark/ccm_proc/bhb_ccm_proc_writecontractmaplookup.py`) submitted via Dataproc Serverless.

### Risks and manual steps
* **Downstream dependency not yet migrated:** `DW.CCM_PROC_JP` is marked as "not yet migrated". Triggering or calling logic cannot be fully wired until the parent workflow is migrated.
* **Separation of execution layers:** This design pass only covers the orchestration XML (`DW.CCM_WRITE_CONTRACTMAPLOOKUP.xml`). The core execution logic within the wrapper script (`BHB_CCM_PROC_WriteContractMapLookup.ksh`) and the Ab Initio graph (`BHB_CCM_PROC_WriteContractMapLookup.mp`) must be migrated in their respective design passes before the Dataproc submission operator in this DAG can target the finalized PySpark file.

---

GRAPH: tmpq927z0yg

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


# DESIGN DOCUMENT: Graph tmpq927z0yg

## 1. GRAPH OVERVIEW
The overall purpose of this graph is to read contract map attribute data from a database table source, apply a straight pass-through formatting transformation, and sort the dataset by the contract identifier (`vertrags_id`). The resulting sorted dataset is then saved to a lookup file target for downstream consumption. Additionally, there is an uncompleted parallel/utility execution flow where data from an external command execution is reformatted and passed to an unconfigured database join before being sent to trash.

---

## 2. SOURCES
* **Label:** Contract Map Lookup File (Kind: table)
  * `DWH$TA_L_MAP_VT_CARM_DWH`
* **Label:** Sort (Kind: select)
  * `DWH$TA_L_MAP_VT_CARM_DWH`

---

## 3. TRANSFORMS
* **Label:** Extract Contract map attributes (Type: reformat)
  * **Expression:**
    ```
    out::reformat(in) =
    begin
      out.* :: in.*;
    end;
    ```
  * **Description:** Passes all input fields from the source table through unmodified.
* **Label:** Reformat (Type: reformat)
  * **Expression:**
    ```
    out::reformat(in) =
    begin
      out.* :: in.*;
    end;
    ```
  * **Description:** Passes all input fields from the Run Program output stream through unmodified.
* **Label:** Sort (Type: sort)
  * **Expression:** `keys=vertrags_id`
  * **Description:** Sorts the incoming dataset by the contract identifier (`vertrags_id`) in ascending order.
* **Label:** Join with DB (Type: join_with_db)
  * **Expression:** *(Not extracted)*
  * **Description:** # REVIEW: DB-LOOKUP SQL NOT EXTRACTED — Performs a live database lookup join on the incoming stream before discarding output to trash.

---

## 4. IN-MEMORY LOOKUPS
*(None extracted)*

---

## 5. FILTERS (select_expr)
*(None extracted)*

---

## 6. OUTPUT TARGETS
* **Label:** Contract Map Lookup File
  * **Kind:** file
  * **Table or path:** `Contract Map Lookup File`
  * **Confirmed Source (from Edges):** Sort
  * **SQL:** 
    ```sql
    # REVIEW: file to Contract Map Lookup File — SQL not extracted; supply manually
    ```
* **Label:** Trash
  * **Kind:** file (discard)
  * **Table or path:** `Trash`
  * **Confirmed Source (from Edges):** Join with DB
  * **SQL:**
    ```sql
    # REVIEW: file to Trash — SQL not extracted; supply manually
    ```

---

## 7. DB JOINS
* **Label:** Join with DB
  * **Query SQL:**
    ```sql
    # REVIEW: DB-LOOKUP SQL NOT EXTRACTED — supply this query manually before running
    ```
  * **Output Column Mapping:**
    ```
    # REVIEW: DB-LOOKUP SQL NOT EXTRACTED
    ```

---

## 8. BUSINESS SUMMARY
* **Read Contract Maps:** Reads full contract map records from the source table `DWH$TA_L_MAP_VT_CARM_DWH`.
* **Reformat/Extract Attributes:** Forwards the attributes of the database table without modifying any values or column structures.
* **Sort by Contract ID:** Orders the contract map stream ascendingly by the primary system contract key (`vertrags_id`).
* **Materialize Lookup Dataset:** Persists the sorted records to the physical reference dataset target `Contract Map Lookup File`.
* **Utility Subflow Execution:** Runs an external task utility (`Run Program`), reformats its output, and routes it to an unconfigured database join component (`Join with DB`) before sending the results to the standard discard node (`Trash`).

---

# PSEUDOCODE OUTLINE

```python
# Step 1: Read the source database table
df_dwh_source = spark.read.format("bigquery") \
    .option("table", "BIGQUERY_SOURCE_DS.DWH$TA_L_MAP_VT_CARM_DWH") \
    .load()
df_dwh_source.createOrReplaceTempView("vw_dwh_ta_l_map_vt_carm_dwh")

# Step 2: Extract Contract Map Attributes (Straight Reformat)
# REVIEW-STRUCT: Column schema not extracted; SELECT * used as placeholder
df_extract_contract_map_attributes = spark.sql("""
    SELECT * 
    FROM vw_dwh_ta_l_map_vt_carm_dwh
""")
df_extract_contract_map_attributes.createOrReplaceTempView("vw_extract_contract_map_attributes")

# Step 3: Sort by contract map key (Sort Component)
# Sort ordering is required for downstream lookup consumption
df_sort = spark.sql("""
    SELECT * 
    FROM vw_extract_contract_map_attributes
    ORDER BY vertrags_id ASC
""")
df_sort.createOrReplaceTempView("vw_sort")

# Step 4: Write to Contract Map Lookup File Target
# REVIEW: Confirm output storage location path for the target lookup file
write_to_bq(df_sort, "Contract_Map_Lookup_File")


# ==========================================
# UTILITY/SECONDARY DISCARD PATHWAY
# ==========================================

# Step 5: Read from Run Program utility
# REVIEW-STRUCT: External process source schema not extracted; supply manually
df_run_program = spark.createDataFrame([], schema=None)
df_run_program.createOrReplaceTempView("vw_run_program")

# Step 6: Reformat Run Program outputs
# REVIEW-STRUCT: Column schema not extracted; SELECT * used as placeholder
df_reformat = spark.sql("""
    SELECT * 
    FROM vw_run_program
""")
df_reformat.createOrReplaceTempView("vw_reformat")

# Step 7: DB Join
# REVIEW: DB-LOOKUP SQL NOT EXTRACTED — supply this SQL manually before running

# Step 8: Write to Trash
# REVIEW: Discard stream to Trash
# write_to_bq(df_join_with_db, "Trash")
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.mp` | `vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.py` | PySpark pipeline migrating the Ab Initio graph logic to extract and sort contract map attributes, persist them to GCS, and trigger the loading timestamp update procedure on BigQuery. |

---

# ADD CONTEXT THE MCP COULD NOT SEE

### Job Dependencies
* **Upstream**:
  * Shared Files: `vobs/dw_source/istools/seu/template` (contains `.dw_global` and `.dw_init`) — Already migrated and merged. Environment variables and global properties are imported/referenced via GCP/Composer configurations.
* **Downstream**:
  * `DW.CCM_PROC_JP` — **Not yet migrated**. This job is a sub-module called inside the downstream parent orchestrator `DW.CCM_PROC_JP`. It must be wired as a task/task group within that downstream parent DAG once it is migrated.

### Execution Order
The legacy orchestration sequence must be preserved within Cloud Composer (Airflow) as follows:
1. **Orchestration Context**: UC4 Export `DW.CCM_WRITE_CONTRACTMAPLOOKUP.xml` maps to the Airflow task state instantiation under parent DAG `DW.CCM_PROC_JP`.
2. **Wrapper Logic**: KSH wrapper `BHB_CCM_PROC_WriteContractMapLookup.ksh` is replaced by an Airflow Dataproc Serverless submit operator passing execution arguments.
3. **Graph Execution**: Ab Initio Graph `BHB_CCM_PROC_WriteContractMapLookup.mp` is converted to a PySpark application running on Dataproc Serverless: `vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.py`.

### Scheduling
* **Triggering Mechanism**: This job has no direct standalone scheduler or trigger. It runs as an included module orchestrated inside parent workflow `DW.CCM_PROC_JP`.
* **Target Alignment**: The migrated PySpark job must remain an importable/callable task unit in Cloud Composer with no independent standalone cron schedule.

### Schedule & Variables — Must Be Retained
* **Variables / Arguments**:
  * `BHB_CCM_PROC_TargetObjectName` (default: `"ContractMapLookup.txt"`): Job-specific output filename.
  * `BHB_CCM_PROC_FirstDay` (e.g. `20050217`): Loading execution window start date.
  * `BHB_CCM_PROC_LastDayPlus1` (e.g. `20050218`): Loading execution window end date.
* **Passing Mechanism**: These values must reach the PySpark application via script arguments (`--target_object_name`, `--first_day`, `--last_day_plus_1`) supplied by the calling Airflow DAG task operator.

### Lineage
* **Upstream Producers (Sources)**:
  * Oracle table `DWH$TA_L_MAP_VT_CARM_DWH` maps to BigQuery table `BQ_DATASET.DWH_TA_L_MAP_VT_CARM_DWH`.
* **Downstream Consumers (Targets)**:
  * `ContractMapLookup.txt` lookup file maps to a GCS bucket location: `gs://{GCS_BUCKET}/ccm_proc/ContractMapLookup.txt`.

### External System Replacements
* **Oracle DB Tables** $\rightarrow$ BigQuery Tables
* **Oracle Stored Procedure** $\rightarrow$ BigQuery SQL Stored Procedure (`DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio`)
* **Local Linux File Directories** $\rightarrow$ Google Cloud Storage (GCS) buckets (`GCS_BUCKET`)

### Cross-File Dependencies
* **Global Configurations**: `.dw_global` and `.dw_init` from `vobs/dw_source/istools/seu/template` provide system-wide configurations and must be referenced/imported as Airflow environment configurations on Composer.

### Target File Plan
* **Target File Path**: `vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.py`
* **Language**: Python (PySpark)
* **Source File Path**: `vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.mp`
* **Description**: PySpark data pipeline running on Dataproc Serverless. It extracts and sorts contract map data from BigQuery, saves it to GCS, and calls the BigQuery stored procedure to update loading timestamps.

### Environment-Specific Values

#### 1. GLOBAL (Environment-Wide)
These values represent the infrastructure itself and are identical for every job in a given deployment environment. They must be resolved at runtime using standard Airflow/GCP configurations:
* `GCP_PROJECT`: GCP Project ID hosting BigQuery and Dataproc resources.
* `GCS_BUCKET`: Target GCS bucket for landing the output lookup file.
* `BQ_DATASET`: BigQuery dataset containing the source tables.
* `BQ_LOCATION`: Regional location for BigQuery resources.

#### 2. JOB-SPECIFIC
These variables are specific to this task execution and are loaded via parameters:
* `BHB_CCM_PROC_TargetObjectName`: Output file name (default `"ContractMapLookup.txt"`).
* `BHB_CCM_PROC_FirstDay`: Execution parameter for loading range start date.
* `BHB_CCM_PROC_LastDayPlus1`: Execution parameter for loading range end date.

---

### Risks and Manual Steps

* **Unmigrated Downstream Orchestrator**: 
  The orchestration logic and parent DAG `DW.CCM_PROC_JP` have not yet been migrated. Sensor and execution linkages cannot be finalized or validated until the parent DAG exists on the target environment.
  * **Risks & Manual Actions Listing**:
    * UPSTREAM: NOT FOUND — `DW.CCM_PROC_JP` — no candidate

* **Stored Procedure Migration**:
  The Ab Initio graph contains a step `Join with DB` in the `Update Loading Timestamps` subgraph that calls:
  `execute :result = DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio(:TARGET_OBJECT_NAME, :FIRST_DAY, :LAST_DAY_PLUS_1)`
  * *Risk*: The PL/SQL logic of the stored procedure `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` is external to this graph and must be migrated to BigQuery separately.
  * *Manual Action*: Confirm that the equivalent BigQuery Stored Procedure `BQ_DATASET.SetzeLadedatumAbInitio` has been pre-created. The PySpark script must invoke this procedure using the BigQuery Python Client or Spark SQL BQ connector.

* **Source Schema Dependencies**:
  The Oracle table `DWH$TA_L_MAP_VT_CARM_DWH` schema must be migrated and populated in BigQuery before executing this pipeline.

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
REASON: The script is a deployed Ab Initio graph that orchestrates database extraction, data sorting, writing to a flat file, and calling an Oracle database procedure.

EVIDENCE
- Business logic found: KSH custom logic builds and runs an Ab Initio graph (via `mp run`) that extracts data from `DWH$TA_L_MAP_VT_CARM_DWH`, sorts it, writes to a lookup file, and calls `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` to update load timestamps.
- AWK: none
- SQL-expressible: partly (the extraction and stored procedure call are SQL, but the graph execution, file writing, and sorting are orchestrated by Ab Initio utility commands).
- Non-SQL side effects: Writes a physical file defined by `$CCM_PROC_ContractMapLookupFilename`.
- Against this verdict: If the target architecture is entirely BigQuery and all files are represented as tables, this could be migrated to a BigQuery script, but it is safer to use Python to manage the file writing and procedure execution.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The script `BHB_CCM_PROC_WriteContractMapLookup.ksh` is a deployed Ab Initio graph execution script. Its purpose is to extract contract mapping information from the database table `DWH$TA_L_MAP_VT_CARM_DWH`, sort this data by `vertrags_id`, and write the resulting dataset to a flat lookup file (`ContractMapLookup.txt`). Additionally, it executes an Oracle stored procedure `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` to update the loading timestamp status for the target contract map lookup object.

2. INVOCATION CONTEXT
   - **Who calls this script**: Typically invoked by a UC4 job scheduler (JOBS_UNIX object), likely named after the script: `BHB_CCM_PROC_WriteContractMapLookup`. Command line parameters are passed as positional arguments.
   - **UC4 native includes**: None referenced in this extraction.
   - **Environment files sourced**:
     - `.project.ksh` from the project directory (e.g., `. $PROJECT_DIR/.project.ksh` via `__AB_INVOKE_PROJECT`). Sourced with parameters: `execute start` and `execute end`.
       # REVIEW-STRUCT: environment file [.project.ksh] body not supplied — behaviour unknown
     - `ab_catalog_functions.ksh` from `$AB_HOME/bin/` if it exists.
       # REVIEW-STRUCT: environment file [ab_catalog_functions.ksh] body not supplied — behaviour unknown

3. PARAMETERS / INPUTS
   The script evaluates and exports several parameters, falling back to environment values if unset.
   - `DB_TNS_NAME_DWH` (Environment variable/Default) - Oracle TNS connection name for DWH.
   - `DB_USER_DWH` (Environment variable/Default) - Database username for DWH.
   - `DB_PASSWD_DWH` (Environment variable/Default) - Database password for DWH.
   - `DB_DB_VERSION_DWH` (Environment variable/Default) - Database version.
   - `DB_CLIENT_VERSION_DWH` (Environment variable/Default) - DB Client version.
   - `DB_DB_HOME_DWH` (Environment variable/Default) - DB Home path.
   - `DB_TNS_NAME_CRS`, `DB_USER_CRS`, `DB_PASSWD_CRS` - CRS connection details (declared but unused).
   - `DB_TNS_NAME_SGM`, `DB_USER_SGM`, `DB_PASSWD_SGM` - SGM connection details (declared but unused).
   - `DB_TNS_NAME_CADS`, `DB_USER_CADS`, `DB_PASSWD_CADS` - CADS connection details (declared but unused).
   - `DB_TNS_NAME_CACM`, `DB_USER_CACM`, `DB_PASSWD_CACM` - CACM connection details (declared but unused).
   - `BHB_Projektverzeichnis`, `BHB_Graph`, `BHB_Prozesstyp`, `BHB_Eintragsnr`, `BHB_Quellverzeichnis`, `BHB_Zielverzeichnis`, `BHB_Dateimaske`, `BHB_Kopfdatensatzkennung`, `BHB_Nutzdatensatzkennung`, `BHB_Endedatensatzkennung`, `BHB_Dateiname` - Framework metadata variables (declared but unused).
   - `BHB_CCM_PROC_TargetObjectName` (Environment variable / Literal default: `ContractMapLookup.txt`) - Used as target object name parameter.
   - `BHB_CCM_PROC_FirstDay` (Environment variable / Literal default: `20050217`) - Used as loading window start date.
   - `BHB_CCM_PROC_LastDayPlus1` (Environment variable / Literal default: `20050218`) - Used as loading window end date.
   - `CCM_PROC_ContractMapLookupFilename` (Environment variable) - Holds target output path.
   - `CCM_PROC_ContractMapLookupDML` (Environment variable) - Path to lookup DML (metadata).
   - `BHB_DB` (Environment variable) - Base path containing DB configurations like `DWH_BHB.dbc`.

   # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `m_env -get AB_GRAPH_SCRIPT_REPOSIT_TRACKING` - Checks repository tracking.
   - `air sandbox find "${PROJECT_DIR}" -project` - EME repository query (Ab Initio specific).
   - `run-and-reposit` - Proprietary Ab Initio script execution tracker.
   - `mp` (various subcommands: `job`, `metadata`, `ofile`, `itable`, `reformat-transform`, `add-port`, `local-sort`, `filter`, `db-lookup`, `broadcast`, `straight-flow`, `run`, `reset`) - Ab Initio command line graph development and runtime engine.
   - `m_db_layout` - Database layout configuration helper.
   - `m_rmcatalog`, `m_mkcatalog` - Temporary Ab Initio catalog management tools.

   *Resolvability*: This script qualifies as a **RESOLVABLE LAUNCHER** because the entire data transformation logic defined dynamically via `mp` components can be directly implemented using a Python script with native database clients and file-handling operations.

5. EMBEDDED SQL
   - **Query 1**
     - Source: Inline DML/mp definition (`DWH_TA_L_MAP_VT_CARM_DWH__table_`)
     - SQL Text: `SELECT * FROM DWH$TA_L_MAP_VT_CARM_DWH`
     - Statement type: `SELECT`
     - Tables touched: `DWH$TA_L_MAP_VT_CARM_DWH`
     - Dialect: Oracle (unambiguously identified by `DWH$` prefix and associated TNS connection parameters)
   - **Query 2 (Stored Procedure call)**
     - Source: `Update_Loading_Timestamps.Join_with_DB` db-lookup definition
     - SQL Text: `execute :result = DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio(:TARGET_OBJECT_NAME, :FIRST_DAY, :LAST_DAY_PLUS_1)`
     - Statement type: Stored Function Call (PL/SQL execution)
     - Tables touched: Unknown (internal to `DWH$PA_ALIS_OBJEKT` package)
     - Dialect: Oracle

6. CONTROL FLOW
   1. Initialize environment variables (`AB_HOME`, `PATH`, etc.) and define internal Ab Initio functions.
   2. Invoke project start initialization using `.project.ksh execute start`.
   3. Check for `-help` argument; exit with status 1 if present.
   4. Evaluate all DB and Framework variables. Set defaults for local settings (`BHB_CCM_PROC_TargetObjectName`, `BHB_CCM_PROC_FirstDay`, `BHB_CCM_PROC_LastDayPlus1`).
   5. Create temporary proxy directory `_AB_PROXY_DIR` to hold temporary DML metadata and transformation code.
   6. Execute the Ab Initio graph pipeline via `mp run`:
      a. Establish temporary lookup catalogs.
      b. Query database table `DWH$TA_L_MAP_VT_CARM_DWH`.
      c. Sort output by key `{vertrags_id}`.
      d. Write results to physical output file `$CCM_PROC_ContractMapLookupFilename`.
      e. Execute stored procedure `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` passing target object, first day, and last day plus 1.
      f. Clean up temporary catalogs.
   7. Remove temporary proxy directory `_AB_PROXY_DIR`.
   8. Invoke project end termination using `.project.ksh execute end`.
   9. Exit with the final return code of the pipeline.

7. ERROR HANDLING & EXIT CODES
   - The script sets traps on `EXIT`, `HUP`, `INT`, `QUIT`, `TERM` to clean up temporary directories (`_AB_PROXY_DIR` and `AB_EXTERNAL_PROXY_DIR`).
   - Checks return status of variable assignments (`mpjret=$?`) and the final `mp run`. Any non-zero status results in immediate exit with the failure code.
   - Successful execution exits with code `0`.
   - Python equivalent:
     - Wrap database operations in `try-except` blocks utilizing client-specific driver exceptions (e.g., `oracledb.DatabaseError`).
     - Utilize a `finally` block or `atexit.register` to perform file cleanup.
     - Raise exceptions or call `sys.exit(code)` to propagate failures.

8. OUTPUTS / SIDE EFFECTS
   - Writes sorted contract mapping data to physical flat file: `$CCM_PROC_ContractMapLookupFilename` (usually mapped to `ContractMapLookup.txt`).
   - Updates target object loading database state using stored function: `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio`.

9. BUSINESS SUMMARY
   - Extracts all active contract mapping identifier pairs (`vertrags_id`, `dwh_vertrag_id`) from the core Data Warehouse database.
   - Sorts the dataset by the business identifier key `vertrags_id` to ensure integrity and fast lookups for subsequent processing stages.
   - Saves this extracted mapping to a shared reference lookup file.
   - Formally registers the completion and temporal window limits of the load execution within the database tracking layer using a central metadata procedure.

=======================================================================================
PYTHON PSEUDOCODE OUTLINE
=======================================================================================

```python
import os
import sys
import shutil
import tempfile
import oracledb  # Provisional choice for Oracle DB client

# REVIEW: target database platform not specified; DB-client library choice below is provisional

# Step 1: Initialize environment and global settings
_AB_PROXY_DIR = None
exit_status = 0

# Step 2: Define cleanup routine
def cleanup():
    global _AB_PROXY_DIR
    if _AB_PROXY_DIR and os.path.exists(_AB_PROXY_DIR):
        shutil.rmtree(_AB_PROXY_DIR)

# Register cleanup for execution termination
import atexit
atexit.register(cleanup)

try:
    # Step 3: Source project environment initialization
    # # REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown; do not guess their names or values
    # In a fully migrated environment, this step might load environment variables from a .env or vault.
    
    # Step 4: Handle help argument
    if len(sys.argv) > 1 and sys.argv[1] == "-help":
        sys.exit(1)

    # Step 5: Read and evaluate parameters
    db_user = os.environ.get("DB_USER_DWH")
    db_password = os.environ.get("DB_PASSWD_DWH")
    db_tns = os.environ.get("DB_TNS_NAME_DWH")
    
    target_object_name = os.environ.get("BHB_CCM_PROC_TargetObjectName", "ContractMapLookup.txt")
    first_day = os.environ.get("BHB_CCM_PROC_FirstDay", "20050217")
    last_day_plus_1 = os.environ.get("BHB_CCM_PROC_LastDayPlus1", "20050218")
    
    output_filename = os.environ.get("CCM_PROC_ContractMapLookupFilename")
    if not output_filename:
        raise ValueError("CCM_PROC_ContractMapLookupFilename environment variable is not defined.")

    # Create proxy directory equivalent for temporary process structures
    _AB_PROXY_DIR = tempfile.mkdtemp(prefix="BHB_CCM_PROC_WriteContractMapLookup-ProxyDir-")

    # Step 6: Connect to the Oracle database
    # # REVIEW-STRUCT: connection parameters inferred from cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying
    connection = oracledb.connect(user=db_user, password=db_password, dsn=db_tns)
    cursor = connection.cursor()

    # Step 7: Execute DB query to retrieve contract maps
    query = "SELECT vertrags_id, dwh_vertrag_id FROM DWH$TA_L_MAP_VT_CARM_DWH"
    cursor.execute(query)
    records = cursor.fetchall()

    # Step 8: Sort data by vertrags_id (ascending) as required by Sort component
    # Record schema: vertrags_id (index 0), dwh_vertrag_id (index 1)
    # Filter out or handle nulls if required, then sort
    sorted_records = sorted(records, key=lambda x: x[0] if x[0] is not None else 0)

    # Step 9: Write sorted records to the lookup file (with delimiter \001 as per DML)
    # The output format is delimited with "\001" and newline "\n"
    with open(output_filename, "w", encoding="utf-8") as outfile:
        for rec in sorted_records:
            v_id = str(int(rec[0])) if rec[0] is not None else ""
            dwh_id = str(int(rec[1])) if rec[1] is not None else ""
            outfile.write(f"{v_id}\x01{dwh_id}\n")

    # Step 10: Call the Oracle Stored Function to update loading timestamps
    # execute :result = DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio(:TARGET_OBJECT_NAME, :FIRST_DAY, :LAST_DAY_PLUS_1)
    # In PL/SQL, this function returns a number
    result_var = cursor.var(oracledb.NUMBER)
    plsql_block = """
    BEGIN
        :result := DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio(:target_obj, :first_d, :last_d);
    END;
    """
    cursor.execute(plsql_block, {
        "result": result_var,
        "target_obj": target_object_name,
        "first_d": first_day,
        "last_d": last_day_plus_1
    })
    
    execution_result = result_var.getvalue()
    print(f"Timestamp update registration completed with result: {execution_result}")
    
    # Commit database changes
    connection.commit()
    cursor.close()
    connection.close()

except Exception as err:
    print(f"Error during execution: {str(err)}", file=sys.stderr)
    exit_status = 1
    sys.exit(exit_status)

finally:
    # Step 11: Sourcing project end environment
    # # REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown; do not guess their names or values
    # Equivalent teardown functions would be invoked here.
    cleanup()

# Step 12: Final Exit
sys.exit(exit_status)
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.ksh` | `vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.py` | Migrates the KornShell wrapper script and encapsulated Ab Initio graph logic (such as reading, sorting, and metadata updates) into a consolidated Python/PySpark script on Dataproc Serverless. |

# Add Context the MCP Could Not See

### Job Dependencies
* **Upstream**: 
  * Shared Files: `vobs/dw_source/istools/seu/template` — Already migrated and merged under PR [#852](https://github.com/gurunathan-prodapt/pi-agents/pull/852). The target script must import/reference the logic from this converted template module (specifically `.dw_global` and `.dw_init`).
* **Downstream**: 
  * `DW.CCM_PROC_JP` — This job is not yet migrated. Downstream orchestration wiring cannot be finalized until this consumer is migrated.

### Execution Order
The execution sequence of the legacy components is as follows:
1. UC4 Orchestration XML: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/CCM_PROC/PRODUKTION/DW.CCM_PROC_JP/DW.CCM_WRITE_CONTRACTMAPLOOKUP.xml` (handled in a separate orchestration migration pass)
2. KornShell Wrapper Script: `vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.ksh` (this pass, which translates the encapsulated graph execution)
3. Ab Initio MP Graph: `vobs/dw_source/isdwh/abinitio/ccm_proc/mp/BHB_CCM_PROC_WriteContractMapLookup.mp` (handled in a separate pass)

In the target environment, the Cloud Composer DAG must call the migrated Python script `vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.py` to preserve this order.

### Scheduling
* This job is **NOT** directly triggered by any of the environment's direct schedulers; it is designed to be executed inside scheduled workflows (i.e., as a shared/included module). 
* Do **NOT** create a standalone Cloud Composer DAG trigger schedule for this job. It must remain a callable Python/PySpark task orchestrated by the parent DAG.

### Schedule & Variables
* Because this job is an include, it does not maintain an independent schedule.
* The following variables must be made available to the migrated task at runtime:
  * `BHB_CCM_PROC_TargetObjectName`: Sourced via task parameter/config (Default: `"ContractMapLookup.txt"`).
  * `BHB_CCM_PROC_FirstDay`: Sourced via task parameter/config (Default: `"20050217"`).
  * `BHB_CCM_PROC_LastDayPlus1`: Sourced via task parameter/config (Default: `"20050218"`).
  * `CCM_PROC_ContractMapLookupFilename`: Target location variable, which must resolve to a GCS path (e.g., `gs://{GCS_BUCKET}/ccm_proc/ContractMapLookup.txt`).

### Lineage
* **Upstream Producer**: BigQuery table `DWH_TA_L_MAP_VT_CARM_DWH` (replacing the Oracle table `DWH$TA_L_MAP_VT_CARM_DWH`).
* **Downstream Consumer**: Flat file `ContractMapLookup.txt` stored in GCS.
* **Database Updates**: Stored procedure `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` execution registers loading metadata.

### External System Replacements
* **Oracle Database**: Replaced by **BigQuery** datasets.
* **Oracle Stored Procedure**: Package function `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` is replaced by a BigQuery stored procedure or equivalent metadata-table update statements.
* **Local Shared Storage**: Replaced by **Google Cloud Storage (GCS)** paths.
* **Ab Initio GDE (`mp` commands)**: Logic is replaced by native Python/PySpark operations executed on **Dataproc Serverless**.

### Cross-File Dependencies
* The migrated Python script must import environment initialization variables from the already-migrated `template` modules (`.dw_global` and `.dw_init`).

### Target File Plan
* `vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.py`
  * **Language**: Python (PySpark)
  * **Source**: `vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.ksh`
  * **Note**: No implementation details or pseudocode are restated here; please refer to the automatically attached MCP output.

### Environment-Specific Values

#### GLOBAL (Environment-Wide)
* `GCP_PROJECT`: Sourced via `from airflow.models import Variable; Variable.get("GCP_PROJECT")` or `os.environ.get("GCP_PROJECT")`. Represents the target Google Cloud Project ID.
* `GCS_BUCKET`: Sourced via `Variable.get("GCS_BUCKET")`. Represents the target GCS bucket for staging flat files.
* `BQ_DATASET`: Sourced via environment or task params, representing the target BigQuery dataset containing the source tables.

#### JOB-SPECIFIC
* `BHB_CCM_PROC_TargetObjectName`: Set to `"ContractMapLookup.txt"` inside the job-level config.
* `BHB_CCM_PROC_FirstDay`: Set to `"20050217"` (or passed dynamically based on execution date parameters).
* `BHB_CCM_PROC_LastDayPlus1`: Set to `"20050218"` (or passed dynamically based on execution date parameters).
* `CCM_PROC_ContractMapLookupFilename`: Resolves to `gs://{GCS_BUCKET}/ccm_proc/ContractMapLookup.txt`.

### Risks and Manual Steps
* **Downstream Wiring**: The downstream job `DW.CCM_PROC_JP` is marked as "not yet migrated". This dependency can only be resolved once the downstream pass is completed.
* **Stored Procedure Migration**: The Oracle procedure `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` must be converted to a BigQuery-compatible Stored Procedure (or an equivalent metadata query) before executing the Python script, as the Python job relies on invoking this state update.
* **Template Integration**: Ensure that the Python environment has the paths to the migrated `template` modules (PR [#852](https://github.com/gurunathan-prodapt/pi-agents/pull/852)) correctly configured in its `sys.path`.