# MIGRATION DESIGN DOCUMENT

## DE CLASSIFICATION PATTERN & TARGET ARCHITECTURE
* **No prescribed pattern available — inferred target from source content.**
* **Inferred Target Pattern**: Apache Airflow DAG Orchestrator on Google Cloud Composer, utilizing Cloud Storage (GCS) and BigQuery for data transformation and storage. KornShell wrappers are converted to Airflow BigQuery insert/export operators or specialized PySpark/Python tasks.

---

## 1. VERBATIM MCP TOOL OUTPUT
The migration design for the main orchestration job plan XML (`DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP.xml`) has been generated verbatim by the migration engine:

```markdown
=== Result for local/home/gurunathan_t/test_dataset/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/PRODUKTION/DW.DWH_IKDB/DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP/DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP.xml ===
### SECTION 1 — DESIGN DOCUMENT

#### 1. Overview
This UC4 workflow (`DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP`) manages, serializes, and synchronizes the daily ingestion and export of KEK and master data (Stamm) within the DWH IKDB ecosystem. It structures a sequence of operations starting with the daily imports, running sequential data consolidations, exports, and final SFTP transfers of output files. Since this is an orchestration-level Job Plan (`JOBP`), it coordinates and executes multiple downstream Job Plans.

---

#### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP` | `JOBP` (Job Plan) | Active (`1`) | Jobplan for the coordination of KEK and master date related IN / OUT interfaces |

---

#### 3. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_ikdb_stamm_kek_taeglich_jp` |
| **schedule** | `None` (Schedule is inherited from the upstream `JSCH` or trigger event; not defined in this file) |
| **start_date** | `datetime(2023, 1, 1)` (Placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` (Mapped from `Else="Wait"` on the Sync Object) |
| **is_paused_upon_creation** | `False` (Source `<Active>1</Active>` mapped) |
| **Default Args** | `owner`: `'airflow'`, `retries`: `0`, `retry_delay`: `timedelta(minutes=5)` |

---

#### 4. Task Inventory
This orchestrator workflow maps its child Job Plans directly to `TriggerDagRunOperator` tasks:

| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_ikdb_info_import_taeglich_jp` | `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | None | Wait (`ActFlg="1"`) | None | Downstream DAG |
| `dw_dwh_ikdb_export_stamm_taeglich_jp` | `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | None | Wait (`ActFlg="1"`) | None | Downstream DAG |
| `dw_dwh_ikdb_stamm_nachlieferung_export_jp` | `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | None | Wait (`ActFlg="1"`) | None | Downstream DAG |
| `dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp` | `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | None | Wait (`ActFlg="1"`) | None | Downstream DAG |
| `dw_dwh_ikdb_pseudo_nachlieferung_export_jp` | `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | None | Wait (`ActFlg="1"`) | None | Downstream DAG |
| `dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp` | `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | None | Wait (`ActFlg="1"`) | None | Downstream DAG |
| `dw_dwh_ikdb_kek_export_taeglich_jp` | `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | None | Wait (`ActFlg="1"`) | None | Downstream DAG |
| `dw_dwh_ikdb_kek_nachlieferung_export_jp` | `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | None | Wait (`ActFlg="1"`) | None | Downstream DAG |
| `dw_dwh_ikdb_kek_konsolidierung_taeglich_jp` | `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | None | Wait (`ActFlg="1"`) | None | Downstream DAG |
| `dw_dwh_ikdb_kek_out_tmd_sftp_jp` | `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | None | Wait (`ActFlg="1"`) | None | Downstream DAG |
| `dw_dwh_ikdb_pseudo_out_tmd_sftp_jp` | `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | None | Wait (`ActFlg="1"`) | None | Downstream DAG |
| `dw_dwh_ikdb_stamm_out_tmd_sftp_jp` | `TriggerDagRunOperator` | N/A | N/A | 0 | N/A | None | None | Wait (`ActFlg="1"`) | None | Downstream DAG |

---

#### 5. Task Dependency Map

The workflow has a linear processing chain that splits into parallel downstream transfer branches at the end:

```
start 
  >> dw_dwh_ikdb_info_import_taeglich_jp 
  >> dw_dwh_ikdb_export_stamm_taeglich_jp 
  >> dw_dwh_ikdb_stamm_nachlieferung_export_jp 
  >> dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp 
  >> dw_dwh_ikdb_pseudo_nachlieferung_export_jp 
  >> dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp 
  >> dw_dwh_ikdb_kek_export_taeglich_jp 
  >> dw_dwh_ikdb_kek_nachlieferung_export_jp 
  >> dw_dwh_ikdb_kek_konsolidierung_taeglich_jp
```

From **`dw_dwh_ikdb_kek_konsolidierung_taeglich_jp` (Lnr 10)**, the flow triggers execution of:
* **`dw_dwh_ikdb_kek_out_tmd_sftp_jp`** (Lnr 11, Row 1, Predecessor: Lnr 10)

From **`dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp` (Lnr 5)**, the flow triggers:
* **`dw_dwh_ikdb_stamm_out_tmd_sftp_jp`** (Lnr 12, Row 3, Predecessor: Lnr 5)

From **`dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp` (Lnr 7)**, the flow triggers:
* **`dw_dwh_ikdb_pseudo_out_tmd_sftp_jp`** (Lnr 13, Row 2, Predecessor: Lnr 7)

##### Ending Consolidation:
`dw_dwh_ikdb_kek_out_tmd_sftp_jp`, `dw_dwh_ikdb_stamm_out_tmd_sftp_jp`, and `dw_dwh_ikdb_pseudo_out_tmd_sftp_jp` must all complete successfully before the workflow reaches the `end` step.

---

#### 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP` | UC4 Workflow Name | `dw_dwh_ikdb_stamm_kek_taeglich_jp` (DAG ID) |
| `DW.DWH_IKDB_INFO_IMPORT_TAEGLICH_JP` | Task Object | `dw_dwh_ikdb_info_import_taeglich_jp` |
| `DW.DWH_IKDB_EXPORT_STAMM_TAEGLICH_JP` | Task Object | `dw_dwh_ikdb_export_stamm_taeglich_jp` |
| `DW.DWH_IKDB_STAMM_NACHLIEFERUNG_EXPORT_JP` | Task Object | `dw_dwh_ikdb_stamm_nachlieferung_export_jp` |
| `DW.DWH_IKDB_STAMM_KONSOLIDIERUNG_TAEGLICH_JP` | Task Object | `dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp` |
| `DW.DWH_IKDB_PSEUDO_NACHLIEFERUNG_EXPORT_JP` | Task Object | `dw_dwh_ikdb_pseudo_nachlieferung_export_jp` |
| `DW.DWH_IKDB_PSEUDO_KONSOLIDIERUNG_TAEGLICH_JP` | Task Object | `dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp` |
| `DW.DWH_IKDB_KEK_EXPORT_TAEGLICH_JP` | Task Object | `dw_dwh_ikdb_kek_export_taeglich_jp` |
| `DW.DWH_IKDB_KEK_NACHLIEFERUNG_EXPORT_JP` | Task Object | `dw_dwh_ikdb_kek_nachlieferung_export_jp` |
| `DW.DWH_IKDB_KEK_KONSOLIDIERUNG_TAEGLICH_JP` | Task Object | `dw_dwh_ikdb_kek_konsolidierung_taeglich_jp` |
| `DW.DWH_IKDB_KEK_OUT_TMD_SFTP_JP` | Task Object | `dw_dwh_ikdb_kek_out_tmd_sftp_jp` |
| `DW.DWH_IKDB_STAMM_OUT_TMD_SFTP_JP` | Task Object | `dw_dwh_ikdb_stamm_out_tmd_sftp_jp` |
| `DW.DWH_IKDB_PSEUDO_OUT_TMD_SFTP_JP` | Task Object | `dw_dwh_ikdb_pseudo_out_tmd_sftp_jp` |

---

#### 7. Error Handling and Retry Strategy
* **Sync Object**: The workflow references `DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP_SYNC` with `Else="Wait"`. This is mapped by configuring `max_active_runs=1` on the DAG, preventing concurrent executions while queuing subsequent execution attempts.
* **Retries**: No explicit retry properties exist in the XML structure for individual tasks. The tasks default to `0` retries, inheriting from the DAG defaults.
* **Task Outcomes**: Because `ActFlg="1"` is configured for all tasks, the orchestrator DAG blocks and waits for completion of each sub-workflow (`wait_for_completion=True`).

---

#### 8. Developer Notes
* **Scheduling**: This XML file is a Job Plan (`JOBP`) and does not contain a scheduler event definition (`EVNT_TIME` or `JSCH`). The DAG's `schedule` is defined as `None` (manual or externally triggered).
* **Execution Environment**: All tasks are sub-workflows (Trigger DAG runs). No Unix tasks (`JOBS_UNIX`) were defined in this XML payload; thus, no Dataproc configuration placeholders are assigned to these tasks.
* **Sub-DAG Status Checking**: Ensure that all downstream target DAG IDs (`dw_dwh_ikdb_*`) exist in the environment, as `TriggerDagRunOperator` will fail to execute if target schemas are missing.

---

### SECTION 2 — PSEUDOCODE

```python
#  Imports 
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

#  Default Args 
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

#  DAG Definition 
dag = DAG(
    dag_id='dw_dwh_ikdb_stamm_kek_taeglich_jp',
    schedule=None,  # No schedule defined in source JOBP
    catchup=False,
    max_active_runs=1,  # Corresponds to Else="Wait" in Sync properties
    is_paused_upon_creation=False,  # Mapped from <Active>1</Active>
    default_args=DEFAULT_ARGS,
    description='Jobplan for coordination of KEK and master data related IN / OUT interfaces'
)

#  Tasks 
start = EmptyOperator(
    task_id='start',
    dag=dag
)

# Col 2, Lnr 2: DW.DWH_IKDB_INFO_IMPORT_TAEGLICH_JP
dw_dwh_ikdb_info_import_taeglich_jp = TriggerDagRunOperator(
    task_id='dw_dwh_ikdb_info_import_taeglich_jp',
    trigger_dag_id='dw_dwh_ikdb_info_import_taeglich_jp',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Col 3, Lnr 3: DW.DWH_IKDB_EXPORT_STAMM_TAEGLICH_JP
dw_dwh_ikdb_export_stamm_taeglich_jp = TriggerDagRunOperator(
    task_id='dw_dwh_ikdb_export_stamm_taeglich_jp',
    trigger_dag_id='dw_dwh_ikdb_export_stamm_taeglich_jp',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Col 4, Lnr 4: DW.DWH_IKDB_STAMM_NACHLIEFERUNG_EXPORT_JP
dw_dwh_ikdb_stamm_nachlieferung_export_jp = TriggerDagRunOperator(
    task_id='dw_dwh_ikdb_stamm_nachlieferung_export_jp',
    trigger_dag_id='dw_dwh_ikdb_stamm_nachlieferung_export_jp',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Col 5, Lnr 5: DW.DWH_IKDB_STAMM_KONSOLIDIERUNG_TAEGLICH_JP
dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp = TriggerDagRunOperator(
    task_id='dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp',
    trigger_dag_id='dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Col 6, Lnr 6: DW.DWH_IKDB_PSEUDO_NACHLIEFERUNG_EXPORT_JP
dw_dwh_ikdb_pseudo_nachlieferung_export_jp = TriggerDagRunOperator(
    task_id='dw_dwh_ikdb_pseudo_nachlieferung_export_jp',
    trigger_dag_id='dw_dwh_ikdb_pseudo_nachlieferung_export_jp',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Col 7, Lnr 7: DW.DWH_IKDB_PSEUDO_KONSOLIDIERUNG_TAEGLICH_JP
dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp = TriggerDagRunOperator(
    task_id='dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp',
    trigger_dag_id='dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Col 8, Lnr 8: DW.DWH_IKDB_KEK_EXPORT_TAEGLICH_JP
dw_dwh_ikdb_kek_export_taeglich_jp = TriggerDagRunOperator(
    task_id='dw_dwh_ikdb_kek_export_taeglich_jp',
    trigger_dag_id='dw_dwh_ikdb_kek_export_taeglich_jp',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Col 9, Lnr 9: DW.DWH_IKDB_KEK_NACHLIEFERUNG_EXPORT_JP
dw_dwh_ikdb_kek_nachlieferung_export_jp = TriggerDagRunOperator(
    task_id='dw_dwh_ikdb_kek_nachlieferung_export_jp',
    trigger_dag_id='dw_dwh_ikdb_kek_nachlieferung_export_jp',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Col 10, Lnr 10: DW.DWH_IKDB_KEK_KONSOLIDIERUNG_TAEGLICH_JP
dw_dwh_ikdb_kek_konsolidierung_taeglich_jp = TriggerDagRunOperator(
    task_id='dw_dwh_ikdb_kek_konsolidierung_taeglich_jp',
    trigger_dag_id='dw_dwh_ikdb_kek_konsolidierung_taeglich_jp',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Col 11, Lnr 11: DW.DWH_IKDB_KEK_OUT_TMD_SFTP_JP (Predecessor: Lnr 10)
dw_dwh_ikdb_kek_out_tmd_sftp_jp = TriggerDagRunOperator(
    task_id='dw_dwh_ikdb_kek_out_tmd_sftp_jp',
    trigger_dag_id='dw_dwh_ikdb_kek_out_tmd_sftp_jp',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Col 11, Lnr 12: DW.DWH_IKDB_STAMM_OUT_TMD_SFTP_JP (Predecessor: Lnr 5)
dw_dwh_ikdb_stamm_out_tmd_sftp_jp = TriggerDagRunOperator(
    task_id='dw_dwh_ikdb_stamm_out_tmd_sftp_jp',
    trigger_dag_id='dw_dwh_ikdb_stamm_out_tmd_sftp_jp',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Col 11, Lnr 13: DW.DWH_IKDB_PSEUDO_OUT_TMD_SFTP_JP (Predecessor: Lnr 7)
dw_dwh_ikdb_pseudo_out_tmd_sftp_jp = TriggerDagRunOperator(
    task_id='dw_dwh_ikdb_pseudo_out_tmd_sftp_jp',
    trigger_dag_id='dw_dwh_ikdb_pseudo_out_tmd_sftp_jp',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

end = EmptyOperator(
    task_id='end',
    dag=dag
)

#  Dependencies 
# Main processing flow
start >> dw_dwh_ikdb_info_import_taeglich_jp
dw_dwh_ikdb_info_import_taeglich_jp >> dw_dwh_ikdb_export_stamm_taeglich_jp
dw_dwh_ikdb_export_stamm_taeglich_jp >> dw_dwh_ikdb_stamm_nachlieferung_export_jp
dw_dwh_ikdb_stamm_nachlieferung_export_jp >> dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp
dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp >> dw_dwh_ikdb_pseudo_nachlieferung_export_jp
dw_dwh_ikdb_pseudo_nachlieferung_export_jp >> dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp
dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp >> dw_dwh_ikdb_kek_export_taeglich_jp
dw_dwh_ikdb_kek_export_taeglich_jp >> dw_dwh_ikdb_kek_nachlieferung_export_jp
dw_dwh_ikdb_kek_nachlieferung_export_jp >> dw_dwh_ikdb_kek_konsolidierung_taeglich_jp

# Parallel downstream branches mapped precisely to predecessor coordinates
dw_dwh_ikdb_kek_konsolidierung_taeglich_jp >> dw_dwh_ikdb_kek_out_tmd_sftp_jp
dw_dwh_ikdb_stamm_konsolidierung_taeglich_jp >> dw_dwh_ikdb_stamm_out_tmd_sftp_jp
dw_dwh_ikdb_pseudo_konsolidierung_taeglich_jp >> dw_dwh_ikdb_pseudo_out_tmd_sftp_jp

# Synchronized collection at End node
[
    dw_dwh_ikdb_kek_out_tmd_sftp_jp,
    dw_dwh_ikdb_stamm_out_tmd_sftp_jp,
    dw_dwh_ikdb_pseudo_out_tmd_sftp_jp
] >> end
```
```

---

## 2. CROSS-FILE DEPENDENCIES & LINEAGE ASSESSMENT

This migration job relies on a sequence of downstream files and utility includes that must be integrated to form the cohesive orchestration run:

1. **`DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP.xml` (Current Job)**:
   * Acts as the parent orchestrator DAG.
   * Invokes `DW.DWH_IKDB_EXPORT_STAMM_TAEGLICH_JP` as a downstream DAG trigger.
2. **`DW.DWH_IKDB_EXPORT_STAMM_TAEGLICH_JP.xml`**:
   * Downstream orchestrator triggering UNIX export jobs.
   * Invokes Unix-Job `DW.DWH_EXIS_IKDB_STAMM_R.xml`.
3. **`DW.DWH_EXIS_IKDB_STAMM_R.xml`**:
   * Unix job execution script.
   * Includes utility scripts: `DW.HOLE_PFAD` and `DW.LESE_LOG`.
   * Invokes the KornShell script: `r_exp_ikdb.ksh` (with arguments `-q d_ikdb_exp_stamm.sql -j EXIS_IKDB_STAMM_R -f STAMM_OUT_TMD -n 7`).
4. **`r_exp_ikdb.ksh`**:
   * Core KornShell logic that wraps Oracle operations, performs dynamic date math, checks past executions against tracking tables (`DWTK_MELDUNGEN`), and triggers the extraction query `d_ikdb_exp_stamm.sql` (not found).

---

## 3. EXTERNAL SYSTEM REPLACEMENTS (ORACLE & SFTP TO GOOGLE CLOUD)

* **Oracle DB Extraction**: The legacy shell script executes queries directly using SQL*Plus. In Google Cloud, this should be replaced with a **BigQuery Insert Job** (selecting from staging datasets or analytical tables and inserting results into export tables) or **Google Cloud Dataproc** to handle larger queries.
* **Metadata Tracking (`DWTK_MELDUNGEN`)**: The KornShell script checks `DWTK_MELDUNGEN` to verify if the export job has already run for a specific date. This should be mapped to a tracking table within BigQuery (e.g. `metadata_dataset.dwtk_meldungen`) and updated using a SQL operation in Airflow.
* **SFTP / Local File Storage**: Local folders like `$DW_DIR_EXP_IKDB/work/` must be migrated to a **Google Cloud Storage (GCS)** bucket path (e.g., `gs://dwh-export-ikdb/work/`). File transfers (`*SFTP_JP` jobs) should be converted to Airflow’s `SFTPToGCSOperator` or `GCSToSFTPOperator` using pre-configured connection IDs.

---

## 4. ENVIRONMENT-SPECIFIC CONFIGURATIONS & VARIABLES

These dynamic variables should be migrated into **Airflow Variables** or **Airflow Connections**:

* `DWH_HOME` / `HOME` $\rightarrow$ Mapped to Airflow environment home/scripts GCS bucket path.
* `DW_ORAUSER` $\rightarrow$ Replaced with BigQuery dataset references and execution service accounts.
* `gcp_conn_id` $\rightarrow$ Connection identifier for BigQuery/GCS tasks (default: `google_cloud_default`).
* `sftp_conn_id` $\rightarrow$ Airflow Connection mapping targets for external file delivery.

---

## 5. RISKS, UNRESOLVED COMPONENTS & MANUAL ACTIONS

The following assets are identified as unresolved references in the source scanning and need manual stubbing or investigation:

* **SOURCE: NOT FOUND** — `d_ikdb_exp_stamm.sql` — *no candidate* (Critical query logic defining export columns and transformations)
* **SOURCE: NOT FOUND** — `d_ikdb_exp_stamm_kp.sql` — *no candidate*
* **SOURCE: NOT FOUND** — `DW.DWH_IKDB_INFO_IMPORT_TAEGLICH_JP` — *no candidate*
* **SOURCE: NOT FOUND** — `DW.DWH_IKDB_STAMM_NACHLIEFERUNG_EXPORT_JP` — *no candidate*
* **SOURCE: NOT FOUND** — `DW.DWH_IKDB_STAMM_KONSOLIDIERUNG_TAEGLICH_JP` — *no candidate*
* **SOURCE: NOT FOUND** — `DW.DWH_IKDB_PSEUDO_NACHLIEFERUNG_EXPORT_JP` — *no candidate*
* **SOURCE: NOT FOUND** — `DW.DWH_IKDB_PSEUDO_KONSOLIDIERUNG_TAEGLICH_JP` — *no candidate*
* **SOURCE: NOT FOUND** — `DW.DWH_IKDB_KEK_EXPORT_TAEGLICH_JP` — *no candidate*
* **SOURCE: NOT FOUND** — `DW.DWH_IKDB_KEK_NACHLIEFERUNG_EXPORT_JP` — *no candidate*
* **SOURCE: NOT FOUND** — `DW.DWH_IKDB_KEK_KONSOLIDIERUNG_TAEGLICH_JP` — *no candidate*
* **SOURCE: NOT FOUND** — `DW.DWH_IKDB_KEK_OUT_TMD_SFTP_JP` — *no candidate*
* **SOURCE: NOT FOUND** — `DW.DWH_IKDB_STAMM_OUT_TMD_SFTP_JP` — *no candidate*
* **SOURCE: NOT FOUND** — `DW.DWH_IKDB_PSEUDO_OUT_TMD_SFTP_JP` — *no candidate*
* **SOURCE: NOT FOUND** — `DW.CALL_STANDARD` — *no candidate* (Error notification standard script)
* **SOURCE: NOT FOUND** — `DW.DWH_ADM_JOB_MONITOR_START` / `DW.DWH_ADM_JOB_MONITOR_END` — *no candidate* (Monitoring scripts, should be replaced by native Airflow task callbacks or SLA monitoring)

---

## 6. TARGET FILE PLAN

To deploy this job completely on Airflow, the target environment should structure the code as follows:

| Target File Path | Language | Purpose | Source Reference |
|---|---|---|---|
| `dags/dw_dwh_ikdb_stamm_kek_taeglich_jp.py` | Python (Airflow DAG) | Parent orchestration DAG coordinates sequential processing | `DW.DWH_IKDB_STAMM_KEK_TAEGLICH_JP.xml` |
| `dags/dw_dwh_ikdb_export_stamm_taeglich_jp.py` | Python (Airflow DAG) | Sub-DAG initiating the export process tasks | `DW.DWH_IKDB_EXPORT_STAMM_TAEGLICH_JP.xml` |
| `scripts/r_exp_ikdb.py` | Python (BigQuery/GCS Operator wrapper) | Execution wrapper performing date-logic/backfill checks and triggering export sqls | `r_exp_ikdb.ksh` |
| `sql/d_ikdb_exp_stamm.sql` | SQL (BigQuery Dialect) | **Stubbed placeholder** (Original missing) - exports master data | `d_ikdb_exp_stamm.sql` (Unresolved) |