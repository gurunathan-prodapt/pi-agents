=== OBJECT: DW.RPOS_CARM_IMPORT (JOBS_UNIX) ===
active=1
title=Job startet AbInitio Graph  map_rpos_carmen_import
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=1
launcher_type=abinitio_graph
launcher_details={'job_arg': 'RPOS_CARM_IMPORT', 'key': '$HOME/aktuell/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg', 'job_type': None}
script_body:
:inc DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='RPOS_CARM_IMPORT'
. $HOME/.dw_init

$HOME/aktuell/abinitio/bin/r_ai_start -j RPOS_CARM_IMPORT -k $HOME/aktuell/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg -n

:inc DW.LESE_LOG
:inc DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Design Document: UC4 to Apache Airflow Migration — DW.RPOS_CARM_IMPORT

## 1. Overview
This migration design document covers the transition of the UC4 UNIX job `DW.RPOS_CARM_IMPORT` to an Apache Airflow DAG. The original UC4 object executes an Ab Initio graph (`map_rpos_carmen_import`) configured to import Carmen-related retail point of sale (RPOS) data. Because this extraction bundle contains only a standalone `JOBS_UNIX` object without a parent workflow (`JOBP`) or schedule (`JSCH`/`EVNT`), the job is designed as a standalone, single-task Airflow DAG. It is classified as externally triggered, and its logic is mapped to run as a PySpark application on Google Cloud Dataproc.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.RPOS_CARM_IMPORT` | JOBS_UNIX | 1 (Active) | Job startet AbInitio Graph  map_rpos_carmen_import |

---

## 3. Scheduling
* **Schedule Source:** No schedule object (`EVNT_TIME` or `JSCH`) or parent `JOBP` is present in this extraction bundle.
* **Trigger Mechanism:** This DAG is designated as **externally triggered** (source unknown from this extraction alone).
* **Airflow Schedule:** `schedule=None`

---

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_rpos_carm_import` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1)* |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `rpos_carm_import` | `DW.RPOS_CARM_IMPORT` | `DataprocSubmitJobOperator` | `gs://YOUR_BUCKET_NAME/pyspark_scripts/map_rpos_carmen_import.py` | `project_id`, `region`, `cluster_name` *(Placeholders)* | 1 | 5 mins | None | None | False | None | Ab Initio graph mapped to Dataproc PySpark task |

---

## 6. Task Dependency Map
Since this DAG contains a single task representing the migrated `JOBS_UNIX` object, the execution flow contains no execution branches:

```python
rpos_carm_import
```

---

## 7. Sync / Concurrency Analysis
No sync rows or concurrency limits (`sync_rows` or cross-locks) were defined for this standalone Unix job in the extraction. Safe execution is enforced using `max_active_runs=1` on the DAG level to prevent simultaneous executions of the same ingestion pipeline.

---

## 8. Error Handling and Retry Strategy
* **Retries:** Configured with 1 retry and a 5-minute retry delay via DAG `default_args`.
* **Failure Handling:** No specific postcondition execution or custom `on_failure_callback` was mapped from the extraction body. Standard Airflow task failure notifications should be applied.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value / Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'RPOS_CARM_IMPORT'` | Passed as a job property or environment variable to the PySpark execution block. |
| Key Config | `$HOME/aktuell/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` | Configuration parameters to be extracted and supplied as Spark properties/arguments. |
| Graph Name | `map_rpos_carmen_import` | Sanitized to target script `map_rpos_carmen_import.py`. |

---

## 10. Developer Notes
* # REVIEW: Standing up standalone `JOBS_UNIX` object as its own DAG due to the absence of a parent `JOBP` workflow in the extraction bundle. Confirm if this job should be integrated into a larger sequence once those definitions are available.
* **GCP Infrastructure Placeholders:** Update the placeholder variables (`GCP_PROJECT_ID`, `GCP_REGION`, `DATAPROC_CLUSTER`, and `GCS_BUCKET_NAME`) in the Airflow environment configuration.
* **Ab Initio Migration:** The Ab Initio graph logic defined in `map_rpos_carmen_import.cfg` must be re-implemented as a Spark/PySpark application and uploaded to GCS at `gs://YOUR_BUCKET_NAME/pyspark_scripts/map_rpos_carmen_import.py`.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GCP Configuration ────────────────────────────────────
# REVIEW: GCP Infrastructure parameters are placeholders and must be configured for the target environment
GCP_PROJECT_ID = "YOUR_PROJECT_ID"
GCP_REGION = "YOUR_REGION"
DATAPROC_CLUSTER = "YOUR_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom failure callbacks defined in the extraction

# ── DAG Definition (dw_rpos_carm_import) ──────────────────
with DAG(
    dag_id="dw_rpos_carm_import",
    default_args=DEFAULT_ARGS,
    description="Job startet AbInitio Graph map_rpos_carmen_import",
    schedule_interval=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Guard Task ──────────────────────────────────────────
    # N/A - No self-lock Else=Skip sync detected

    # ── Sensor Task ─────────────────────────────────────────
    # N/A - No earliest_start_time constraint detected

    # ── Calendar Check Task ─────────────────────────────────
    # N/A - No CaleOn=1 constraint detected

    # ── Task: rpos_carm_import ───────────────────────────────
    # Mapped from JOBS_UNIX launcher_type=abinitio_graph
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/map_rpos_carmen_import.py",
            "properties": {
                "spark.yarn.appMasterEnv.DWH_JOB_KENNUNG": "RPOS_CARM_IMPORT"
            }
        }
    }

    rpos_carm_import = DataprocSubmitJobOperator(
        task_id="rpos_carm_import",
        job=pyspark_job_config,
        region=GCP_REGION,
        project_id=GCP_PROJECT_ID,
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single-task workflow execution
    rpos_carm_import
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml` | `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import.py` | Migrated from UC4 UNIX job definition to an Apache Airflow DAG in Cloud Composer. |

---

### Job Dependencies
* **Upstream Dependency:** 
  * `Shared Files — abinitio_pyspark_linked_job/isccr/abinitio/bin` is already migrated and merged under PR `https://github.com/gurunathan-prodapt/pi-agents/pull/767`.
  * In the target platform, the DAG imports/references this shared utility script (or its Python-migrated equivalent) rather than re-implementing it.

---

### Execution Order
The target orchestration (Airflow DAG) preserves the logical legacy call sequence across its tasks:
1. **Initiation (`DW.RPOS_CARM_IMPORT.xml`)**: Handled by the DAG container startup and initial logging setup.
2. **Configuration Parse (`map_rpos_carmen_import.cfg`)**: Handled by passing job-specific parameters directly inside the DAG task definition as task arguments/properties.
3. **Execution Wrapper (`map_rpos_carmen_import.ksh`)**: Bypassed in the target environment as the DAG directly schedules and invokes the Dataproc Serverless PySpark pipeline.
4. **Data Transformation (`map_rpos_carmen_import.mp`)**: Executed as a PySpark application on Dataproc Serverless, triggered via `DataprocSubmitJobOperator`.

---

### Lineage
* **Upstream Producer (Code Utility)**: Uses the utility script `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start` (already migrated separately).
* **Upstream Packages/Includes (Resolved as "No Source Needed")**:
  * `.DW_INIT`
  * `DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC`
  * `DW.HOLE_PFAD`
  * `DW.LESE_LOG`
  * `DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC`
  *(All of these legacy includes have been human-reviewed and confirmed as not needed in the target environment; standard Airflow logging, connection profiles, and environment variables will replace their functionality).*

---

### External System Replacements
* **Legacy Execution Host (`DWHDWH1P`)**: Replaced by standard Google Cloud Composer environments and managed Google Cloud Dataproc Serverless infrastructure.
* **Legacy File Paths ($DW_DIR_IMP_SAP)**: Replaced by cloud-native storage paths on Google Cloud Storage (e.g. `gs://{GCS_BUCKET}/crs/work/` and `gs://{GCS_BUCKET}/crs/store/`).

---

### Cross-File Dependencies
* **Shared Tables**: The data pipeline loads/manipulates the following target DWH tables in BigQuery:
  * `DWH$TA_F_RPOS_CARM`
  * `DWH$TA_F_RPOS_FACT_CARM`
  * `DWH$TA_F_RPOS_RESELLING_CARM`
  * `DWH$TA_F_GPOS_FACT_CARM`
  * `DWH$TA_T_RPOS_CARM`
  * `DWH$TA_K_RECH_ABSGRP`
* **Configuration Parameters**: The Airflow DAG extracts the configurations originally stored in `map_rpos_carmen_import.cfg` and injects them into the Dataproc job operator payload.

---

### Target File Plan
* **Target File**: `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/dw_rpos_carm_import.py` (Airflow Python DAG)
  * **Source**: `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml`
  * **Description**: Airflow DAG orchestrating the retail point of sale (RPOS) Carmen billing data import. It invokes the PySpark conversion of the Ab Initio graph (`map_rpos_carmen_import.py`) using GCS input/output configurations.

*Note: Sibling files in the execution chain (such as the KSH wrapper script and the .mp graph file) are not in the scope of this file's design pass and are converted during their respective migration passes.*

---

### Environment-Specific Values

#### 1. GLOBAL (Environment-Wide Infrastructure)
These variables identify target infrastructure and are uniform across all jobs in the environment. They must be retrieved from the Airflow Variables or environment at runtime.

* **GCP_PROJECT**
  * *Retrieval (Airflow DAG)*: `Variable.get("GCP_PROJECT")`
* **GCP_REGION**
  * *Retrieval (Airflow DAG)*: `Variable.get("GCP_REGION")`
* **DATAPROC_REGION**
  * *Retrieval (Airflow DAG)*: `Variable.get("DATAPROC_REGION")`
* **DATAPROC_CLUSTER**
  * *Retrieval (Airflow DAG)*: `Variable.get("DATAPROC_CLUSTER")`
* **GCS_BUCKET**
  * *Retrieval (Airflow DAG)*: `Variable.get("GCS_BUCKET")`
* **DW_DIR_IMP_SAP** (Base landing directory for SAP files on cloud storage)
  * *Retrieval (Airflow DAG)*: `Variable.get("DW_DIR_IMP_SAP")`

#### 2. JOB-SPECIFIC (Particular to this job/file)
These values are specific to the `dw_rpos_carm_import` job and must be defined as inline constants or task arguments inside the Python DAG file.

* **DWH_JOB_KENNUNG**: `'RPOS_CARM_IMPORT'`
* **BHB_Projektverzeichnis**: `'/Projects/TMD/processing/BHB/BD_PROC'`
* **BHB_Version**: `'RLS_BHB_nach_64_rabatt_sap'`
* **BHB_Graph**: `'map_rpos_carmen_import'`
* **BHB_Prozesstyp**: `'D'`
* **BHB_Quellverzeichnis**: `f"{Variable.get('DW_DIR_IMP_SAP')}/crs/work/"`
* **BHB_Zielverzeichnis**: `f"{Variable.get('DW_DIR_IMP_SAP')}/crs/store/"`
* **BHB_Dateimaske**: `'CARMEN_B_*_pos.fix'`
* **BHB_Kopfdatensatzkennung**: `'H'`
* **BHB_Nutzdatensatzkennung**: `'P'`
* **BHB_Endedatensatzkennung**: `'X'`

---

### Risks and Manual Steps
1. **Missing Downstream PySpark Script**: The Airflow DAG references `gs://{GCS_BUCKET}/pyspark_scripts/map_rpos_carmen_import.py`. This script represents the converted logic of the Ab Initio graph (`map_rpos_carmen_import.mp`) and KSH wrapper, which is handled in a separate design/build pass. The DAG cannot run successfully until that file is deployed.
2. **Airflow Environment Setup**: The global variables (`GCP_PROJECT`, `GCP_REGION`, `DATAPROC_CLUSTER`, `GCS_BUCKET`, and `DW_DIR_IMP_SAP`) must be pre-configured in the Cloud Composer environment variables or Airflow Variables store prior to DAG deployment.
3. **Execution Guarding**: Ensure that `max_active_runs=1` is strictly preserved to prevent parallel execution conflicts on the target BigQuery tables.

---

GRAPH: tmppqg6cj9n

=== SOURCES ===
[DWH$TA_F_RPOS_CARM] kind=select
  select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_CARM
[DWH$TA_F_RPOS_CARM-2] kind=select
  select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_CARM
[DWH$TA_F_RPOS_FACT_CARM] kind=select
  select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_FACT_CARM
[DWH$TA_F_RPOS_FACT_CARM - 2] kind=select
  select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_FACT_CARM
[DWH$TA_F_RPOS_RESELLING_CARM] kind=select
  select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_RESELLING_CARM
[DWH$TA_F_RPOS_RESELLING_CARM-1] kind=select
  select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_RESELLING_CARM
[dwh$ta_c_vertrag] kind=select
  select 
rahmenvertrag_id,
vertrag_id_carmen,
dwh_vertrag_id,
dwh_gp_id,
dwh_konto_id,
dwh_tarifgr_id,
vo_kenn,
zv_id,
gueltig_von, 
gueltig_bis
from 
dwh$ta_c_vertrag
where 
gueltig_bis >= to_date('20050401', 'YYYYMMDD') 
and ABLOCAL(dwh$ta_c_vertrag)

=== LOOKUPS ===
  (none extracted — check .mp file for lookup_file fields)

=== TRANSFORMS ===
[Reformat rechnung_datum to datetime for Delete] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
end;
[Validate Records] type=reformat
  out::reformat(in) =
begin
  out.monats_id :: if(!is_valid(in.monats_id))
force_error("Invalid data format in monats_id")
else
in.monats_id;
  out.rechnung_datum :: if(!is_valid(in.rechnung_datum))
force_error("Invalid data format in rechnung_datum")
else
in.rechnung_datum;
  out.standardvertrags_id :: if(!is_valid(in.standardvertrags_id))
force_error("Invalid data format in standardvertrags_id")
else
in.standardvertrags_id;
  out.vertrags_id :: if(!is_valid(in.vertrags_id))
force_error("Invalid data format in vertrags_id")
else
in.vertrags_id;
  out.rechpos_brutto_eur :: if(!is_valid(in.rechpos_brutto_eur))
force_error("Invalid data format in rechpos_brutto_eur")
else
in.rechpos_brutto_eur;
  out.rechpos_netto_eur :: if(!is_valid(in.rechpos_netto_eur))
force_error("Invalid data format in rechpos_netto_eur")
else
in.rechpos_netto_eur;
  out.rechpos_mwst_eur :: if(!is_valid(in.rechpos_mwst_eur))
force_error("Invalid data format in rechpos_mwst_eur")
else
in.rechpos_mwst_eur;
  out.* :: in.*;
end;
[Reformat for DB] type=reformat
  /*

*/
out::reformat(in) =
begin
  let integer(4) v_abs_grp_pos = 9;
  let integer(4) v_abs_grp_len = 5;
  let string("\\001") tmp_abs_grp =string_substring(in.rechnung_id,v_abs_grp_pos, v_abs_grp_len);
  let string("\\001") tmp_standardvertrags_id =string_lrtrim(in.standardvertrags_id);
  let string("\\001") tmp_vertrags_id =string_lrtrim(in.vertrags_id);

  out.monats_id :: if(is_blank(in.monats_id))
force_error("Invalid Data in field monats_id")
else
(date("YYYYMM"))in.monats_id;
  out.debitor_id :: if(is_blank(in.debitor_id))
force_error("Invalid Data in field debitor_id")
else 
string_lrtrim(in.debitor_id);
  out.rechnung_id :: if(is_blank(in.rechnung_id))
force_error("Invalid Data in field rechnung_id")
else 
string_lrtrim(in.rechnung_id);
  out.rechnung_datum :: if(is_blank(in.rechnung_datum))
force_error("Invalid Data in field rechnung_datum")
else 
(date("YYYYMMDD"))in.rechnung_datum;
  out.standardvertrags_id :: if(is_blank(in.standardvertrags_id)) force_error("Invalid Data in field standardvertrags_id") else
if(tmp_standardvertrags_id != "#") string_lrtrim(tmp_standardvertrags_id);
  out.vertrags_id :: if(is_blank(in.vertrags_id)) force_error("Invalid Data in field vertrags_id") else 
if(tmp_vertrags_id != '#') string_lrtrim(tmp_vertrags_id);
  out.rech_leistung_id_carm :: if(is_blank(in.rech_leistung_id_carm))
force_error("Invalid Data in field rech_leistung_id_carm")
else 
string_lrtrim(in.rech_leistung_id_carm);
  out.rechpos_brutto_eur :: if(is_blank(in.rechpos_brutto_eur))
force_error("Invalid Data in field rechpos_brutto_eur")
else 
in.rechpos_brutto_eur;
  out.rechpos_netto_eur :: if(is_blank(in.rechpos_netto_eur))
force_error("Invalid Data in field rechpos_netto_eur")
else 
in.rechpos_netto_eur;
  out.rechpos_mwst_eur :: if(is_blank(in.rechpos_mwst_eur))
force_error("Invalid Data in field rechpos_mwst_eur")
else 
in.rechpos_mwst_eur;
  out.abs_grp :: if(!is_blank(tmp_abs_grp))
string_lrtrim(tmp_abs_grp);
  out.pooling :: if(!is_blank(in.pooling))
string_lrtrim(in.pooling);
  out.rechnungvertrag_id :: if(!is_blank(in.rechnungvertrag_id))
(decimal("\\n"))string_lrtrim(in.rechnungvertrag_id);
  out.prob_vertrag_id :: if(!is_blank(in.prob_vertrag_id))
string_lrtrim(in.prob_vertrag_id);
  out.prob_provider_kenn :: if(!is_blank(in.prob_provider_kenn))
string_lrtrim(in.prob_provider_kenn);
  out.anz_leistungen :: if(!is_blank(in.anz_leistungen))
(decimal("\\n")) string_lrtrim(in.anz_leistungen);
  out.anz_tickets :: if(!is_blank(in.anz_tickets))
(decimal("\\n")) string_lrtrim(in.anz_tickets);
  out.rpos_geschaftsform_kenn :: if(!is_blank(in.rpos_geschaftsform_kenn))
in.rpos_geschaftsform_kenn;
  out.vas_kenn :: if(!is_blank(in.vas_kenn))
string_lrtrim(in.vas_kenn);
  out.verkauftes_basisprodukt_id :: if(!is_blank(in.kennung5))
string_lrtrim(in.kennung5);
end;
[replace ',' by '.'] type=reformat
  out::reformat(in) =
begin
  out.kennzeichen :: in.kennzeichen;
  out.datensatz_rest :: string_replace(in.datensatz_rest, ',', '.');
end;
[Reformat Referencerecord] type=reformat
  out::reformat(in) =
begin
  out.kennzeichen :: in.kennzeichen;
  out.datensatz_rest :: in.datensatz_rest;
end;
[Reformat Enderecord for Update] type=reformat
  /*Reformat operation*/
out::reformat(in) =
begin
        out.dateiname  :: "$\{BHB_Dateiname\}";
        out.eintragsnr :: $\{BHB_Eintragsnr\};
        out.bemerkung  :: in.bemerkung;
        out.anzahl     :: in.anzahl;
        out.inhalt     :: in.inhalt;
end;
[Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur-1] type=rollup
  /*Das Ladedatum wird auf Seiten von Informatica beim
select via SYSDATE gesetzt. 
Da dies nur ein Datum zurï¿½ckgibt, 
wird hier now1() verwendet.*/
out::rollup(in) =
begin
  out.* :: in.*;
  out.rechpos_brutto_eur :: sum(in.rechpos_brutto_eur);
  out.rechpos_netto_eur :: sum(in.rechpos_netto_eur);
  out.rechpos_mwst_eur :: sum(in.rechpos_mwst_eur);
end;
[Determine rows to be deleted] type=join
  out::join(in0, in1) =
begin
  out.rechnung_id :: in0.rechnung_id;
  out.rechnung_datum :: in0.rechnung_datum;
  out.standardvertrags_id :: in0.standardvertrags_id;
  out.vertrags_id :: in0.vertrags_id;
  out.rech_leistung_id_carm :: in0.rech_leistung_id_carm;
  out.newline :: in0.newline;  /*VARCHAR2(13) NOT NULL*/
end;
[Reformat for delete] type=reformat
  out::reformat(in) =
begin
  out.rechnung_id :: in.rechnung_id;
  out.rechnung_datum :: in.rechnung_datum;
  out.standardvertrags_id :: in.standardvertrags_id;
  out.vertrags_id :: in.vertrags_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
end;
[Determine rows to be deleted] type=join
  out::join(in0, in1) =
begin
  out.rechnung_id :: in0.rechnung_id;
  out.rechnung_datum :: in0.rechnung_datum;
  out.standardvertrags_id :: in0.standardvertrags_id;  /*VARCHAR2(13) NOT NULL*/
  out.vertrags_id :: in0.vertrags_id;
  out.rech_leistung_id_carm :: in0.rech_leistung_id_carm;
  out.newline :: in0.newline;
end;
[Join with DB] type=join_with_db
  select rechnung_id
from DWH$TA_F_RPOS_CARM
where rechnung_id = :rechnung_id
and rechnung_datum = :rechnung_datum
and standardvertrags_id = :standardvertrags_id
and vertrags_id = :vertrags_id 
and rech_leistung_id_carm = :rech_leistung_id_carm

-- OUTPUT MAPPING (query_result = DB join result, in = current stream) --
type query_result_type = 
record
  string(unsigned integer(2)) rechnung_id; /* VARCHAR2(14) NOT NULL*/
end /* Generated type from select statement*/;

out::join_with_db(in, query_result) =
begin
  out.* :: in.*;
  out.delete_flag :: if(is_defined(query_result.rechnung_id))
 1
else
 0;
end;
[Join with DB, Determine rows to be deleted] type=join_with_db
  select
rechnung_id,
rechnung_datum,
standardvertrags_id,
vertrags_id,
rech_leistung_id_carm
from
DWH$TA_F_RPOS_FACT_CARM
where
rechnung_id = :rechnung_id and
rechnung_datum = :rechnung_datum and
standardvertrags_id = :standardvertrags_id and
vertrags_id = :vertrags_id and
rech_leistung_id_carm = :rech_leistung_id_carm

-- OUTPUT MAPPING (query_result = DB join result, in = current stream) --
type query_result_type = 
record
  string(unsigned integer(1)) rechnung_id; /* VARCHAR2(14) NOT NULL*/
  datetime("YYYYMMDDHH24MISS") rechnung_datum; /* DATE NOT NULL*/
  decimal(11) standardvertrags_id; /* NUMBER(10) NOT NULL*/
  decimal(11) vertrags_id; /* NUMBER(10) NOT NULL*/
  string(unsigned integer(1)) rech_leistung_id_carm; /* VARCHAR2(9) NOT NULL*/
end /* Generated type from select statement*/;

out::join_with_db(in, query_result) =
begin
  out.rechnung_id :: query_result.rechnung_id;
  out.rechnung_datum :: query_result.rechnung_datum;
  out.standardvertrags_id :: query_result.standardvertrags_id;
  out.vertrags_id :: query_result.vertrags_id;
  out.rech_leistung_id_carm :: query_result.rech_leistung_id_carm;
end;
[Determine rows to be deleted] type=join
  out::join(in0, in1) =
begin
  out.rechnung_id :: in0.rechnung_id;
  out.rechnung_datum :: in0.rechnung_datum;
  out.standardvertrags_id :: in0.standardvertrags_id;
  out.vertrags_id :: in0.vertrags_id;
  out.rech_leistung_id_carm :: in0.rech_leistung_id_carm;
  out.newline :: in0.newline;  /*VARCHAR2(13) NOT NULL*/
end;
[Reformat for delete] type=reformat
  out::reformat(in) =
begin
  out.rechnung_id :: in.rechnung_id;
  out.rechnung_datum :: in.rechnung_datum;
  out.standardvertrags_id :: in.standardvertrags_id;
  out.vertrags_id :: in.vertrags_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
end;
[Join with DB, Determine rows to be deleted] type=join_with_db
  select
rechnung_id,
rechnung_datum,
standardvertrags_id,
vertrags_id,
rech_leistung_id_carm
from
DWH$TA_F_RPOS_RESELLING_CARM
where
rechnung_id = :rechnung_id and
rechnung_datum = :rechnung_datum and
standardvertrags_id = :standardvertrags_id and
vertrags_id = :vertrags_id and
rech_leistung_id_carm = :rech_leistung_id_carm

-- OUTPUT MAPPING (query_result = DB join result, in = current stream) --
type query_result_type = 
record
  string(unsigned integer(1)) rechnung_id; /* VARCHAR2(14) NOT NULL*/
  datetime("YYYYMMDDHH24MISS") rechnung_datum; /* DATE NOT NULL*/
  decimal(11) standardvertrags_id; /* NUMBER(10) NOT NULL*/
  decimal(11) vertrags_id; /* NUMBER(10) NOT NULL*/
  string(unsigned integer(1)) rech_leistung_id_carm; /* VARCHAR2(9) NOT NULL*/
end /* Generated type from select statement*/;

out::join_with_db(in, query_result) =
begin
  out.rechnung_id :: query_result.rechnung_id;
  out.rechnung_datum :: query_result.rechnung_datum;
  out.standardvertrags_id :: query_result.standardvertrags_id;
  out.vertrags_id :: query_result.vertrags_id;
  out.rech_leistung_id_carm :: query_result.rech_leistung_id_carm;
end;
[Determine rows to be deleted] type=join
  out::join(in0, in1) =
begin
  out.rechnung_id :: in0.rechnung_id;
  out.rechnung_datum :: in0.rechnung_datum;
  out.standardvertrags_id :: in0.standardvertrags_id;
  out.vertrags_id :: in0.vertrags_id;
  out.rech_leistung_id_carm :: in0.rech_leistung_id_carm;
  out.newline :: in0.newline;  /*VARCHAR2(13) NOT NULL*/
end;
[Reformat] type=reformat
  out::reformat(in) =
begin
  out.rechnung_id :: in.rechnung_id;
  out.rechnung_datum :: in.rechnung_datum;
  out.standardvertrags_id :: in.standardvertrags_id;
  out.vertrags_id :: in.vertrags_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
end;
[Determine rows to be deleted] type=join
  out::join(in0, in1) =
begin
  out.rechnung_id :: in0.rechnung_id;
  out.rechnung_datum :: in0.rechnung_datum;
  out.debitor_id :: in0.debitor_id;
  out.newline :: in0.newline;
end;
[Join with DB, Determine rows to be deleted] type=join_with_db
  select
rechnung_id,
rechnung_datum,
debitor_id
from
DWH$TA_T_RPOS_CARM
where
rechnung_id = :rechnung_id and
rechnung_datum = :rechnung_datum and
debitor_id = :debitor_id

-- OUTPUT MAPPING (query_result = DB join result, in = current stream) --
type query_result_type = 
record
  string(unsigned integer(1)) rechnung_id; /* VARCHAR2(14) NOT NULL*/
  datetime("YYYYMMDDHH24MISS") rechnung_datum; /* DATE NOT NULL*/
  string(unsigned integer(1)) debitor_id; /* VARCHAR2(13) NOT NULL*/
end /* Generated type from select statement*/;

out::join_with_db(in, query_result) =
begin
  out.rechnung_id :: query_result.rechnung_id;
  out.rechnung_datum :: query_result.rechnung_datum;
  out.debitor_id :: query_result.debitor_id;
end;
[Determine rows to be deleted (incl. dedup of port 1)] type=join
  out::join(in0, in1) =
begin
  out.rechnung_id :: in0.rechnung_id;
  out.rechnung_datum :: in0.rechnung_datum;
  out.debitor_id :: in0.debitor_id;
  out.newline :: in0.newline;
end;
[Filter out where rpos_geschaeftsform_kenn != 'S'] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
end;
[Join with dwh$ta_c_vertrag-1] type=join
  out::join(in0, in1) =
begin
  out.* :1: in0.*;
  out.* :: in1.*;
  out.dwh_vertrag_id :: if(!is_null(in1.dwh_vertrag_id))
 in1.dwh_vertrag_id;
  out.dwh_gp_id :: if(!is_null(in1.dwh_gp_id))
 in1.dwh_gp_id;
  out.dwh_konto_id :: if(!is_null(in1.dwh_konto_id))
 in1.dwh_konto_id;
  out.dwh_tarifgr_id :: if (!is_null(in1.dwh_tarifgr_id)) 
 in1.dwh_tarifgr_id;
  out.vo_kenn :: if(!is_null(in1.vo_kenn))
 in1.vo_kenn;
  out.zv_id :: if(!is_null(in1.zv_id))
 in1.zv_id;
end;
[Join CSV-File with dwh$TA_C_VERTRAG] type=join
  /*Da fï¿½r die Tabelle DWH$TA_C_VERTRAG
eine Basisschicht existiert dï¿½rfte es
hier nie der Fall sein, dass gueltig_von
und gueltig_bis NULL sind.*/
out::join(in0, in1) =
begin
  out.* :: in0.*;
  out.rahmenvertrag_id :: in1.rahmenvertrag_id;
  out.dwh_vertrag_id :: in1.dwh_vertrag_id;
  out.dwh_gp_id :: in1.dwh_gp_id;
  out.dwh_konto_id :: in1.dwh_konto_id;
  out.dwh_tarifgr_id :: in1.dwh_tarifgr_id;
  out.vo_kenn :: in1.vo_kenn;
  out.zv_id :: in1.zv_id;
  out.gueltig_von :1: in1.gueltig_von;
  out.gueltig_bis :: in1.gueltig_bis;
end;
[Join with dwh$ta_c_vertrag] type=join_with_db
  select 
c.rahmenvertrag_id,
c.dwh_vertrag_id,
c.dwh_gp_id,
c.dwh_konto_id,
c.dwh_tarifgr_id,
c.vo_kenn,
c.zv_id,
c.gueltig_von,
c.gueltig_bis
from 
dwh$ta_c_vertrag c
where
c.vertrag_id_carmen (+) = :vertrags_id and
c.gueltig_bis >= to_date('20050401', 'YYYYMMDD')

-- OUTPUT MAPPING (query_result = DB join result, in = current stream) --
type query_result_type = 
record
  string(unsigned integer(1)) rahmenvertrag_id = NULL; /* VARCHAR2(10)*/
  decimal(17) dwh_vertrag_id; /* NUMBER(16) NOT NULL*/
  decimal(17) dwh_gp_id = NULL; /* NUMBER(16)*/
  decimal(17) dwh_konto_id = NULL; /* NUMBER(16)*/
  decimal(100) dwh_tarifgr_id = NULL; /* NUMBER*/
  string(5) vo_kenn = NULL; /* CHAR(5)*/
  string(unsigned integer(1)) zv_id = NULL; /* VARCHAR2(10)*/
  datetime("YYYYMMDDHH24MISS") gueltig_von; /* DATE NOT NULL*/
  datetime("YYYYMMDDHH24MISS") gueltig_bis = NULL; /* DATE*/
end /* Generated type from select statement*/;

out::join_with_db(in, query_result) =
begin
  out.* :: in.*;
  out.rahmenvertrag_id :: query_result.rahmenvertrag_id;
  out.dwh_vertrag_id :: query_result.dwh_vertrag_id;
  out.dwh_gp_id :: query_result.dwh_gp_id;
  out.dwh_konto_id :: query_result.dwh_konto_id;
  out.dwh_tarifgr_id :: query_result.dwh_tarifgr_id;
  out.vo_kenn :: query_result.vo_kenn;
  out.zv_id :: query_result.zv_id;
  out.gueltig_von :: query_result.gueltig_von;
  out.gueltig_bis :: query_result.gueltig_bis;
end;
[Filter out where rankindex != 1] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
end;
[Scan - Ranking over gueltig_von desc; dwh_vertrag_id desc] type=scan
  type temporary_type = 
record
  integer(1) first_time;
  decimal('\|') rank;
  decimal('\|') rank_increase;
  string('\|') last_gueltig_von = NULL;
  string('\|') last_dwh_vertrag_id = NULL;
end /* Temporary variable*/;


/*This function may be optionally defined. 
Initialize temporary*/
temp::initialize(in) =
begin
  temp.first_time :: 1;
  temp.rank :: 0;
  temp.rank_increase :: 1;
  temp.last_gueltig_von :: "";
  temp.last_dwh_vertrag_id :: "";
end;



/*Do computation*/
temp::scan(temp, in) =
begin
  let decimal('\|') rank = 0;
  let decimal('\|') rank_increase = 0;

if (! temp.first_time && in.gueltig_von == temp.last_gueltig_von && 
                         in.dwh_vertrag_id == temp.last_dwh_vertrag_id )                          
  begin
     rank = temp.rank;
     rank_increase = temp.rank_increase + 1;
  end
  else
  begin
     rank = temp.rank + temp.rank_increase;
     rank_increase = 1;
  end

  temp.first_time :: 0;
  temp.rank :: rank;
  temp.rank_increase :: rank_increase;
  temp.last_gueltig_von :: in.gueltig_von;
  temp.last_dwh_vertrag_id :: in.dwh_vertrag_id;
end;



/*Create output record*/
out::finalize(temp, in) =
begin
  out.* :: in.*;
  out.rankindex :: temp.rank;
  out.monats_id :: (decimal("\\001"))(string("\\001"))in.monats_id;
  out.dwh_vertrag_id :: if(in.dwh_vertrag_id!='\\000')
in.dwh_vertrag_id;
end;
[Filter out invalid data] type=reformat
  /*Im Kontext dwh_vertrag_id und gueltig_von muss
hier fï¿½r die korrekte Durchfï¿½hrung der Rankings 
im Fall von NULL der Wert \\000 (NUL) gesetzt werden.*/
out::reformat(in) =
begin
  let string("\\001") tmp_dwh_vertrag_id =string_concat('000000000000000',in.dwh_vertrag_id);

  out.* :: in.*;
  out.dwh_vertrag_id :: if(is_null(in.dwh_vertrag_id))
'\\000'
else
string_substring(tmp_dwh_vertrag_id,string_length(tmp_dwh_vertrag_id)-15,16);
  out.gueltig_von :: if(is_null(in.gueltig_von))
'\\000'
else
in.gueltig_von;
end;
[Scan - Mark valid historized datasets] type=scan
  type temporary_type = 
record
  integer(1) first_time;
  integer(1) valid_flag;
end /* Temporary variable*/;


/*This function may be optionally defined. 
Initialize temporary*/
temp::initialize(in) =
begin
  temp.first_time :: 1;
  temp.valid_flag :: 1;
end;



/*Do computation*/
temp::scan(temp, in) =
begin
  let integer(1) my_valid_flag = 1;

if(temp.first_time == 1)
    my_valid_flag = 0; 
  else 
  begin
     if (temp.first_time == 0 && !is_null(in.gueltig_von))
     my_valid_flag = 0;
     else
     my_valid_flag = 1;
  end

  temp.first_time :: 0;
  temp.valid_flag :: my_valid_flag;
end;



/*Create output record*/
out::finalize(temp, in) =
begin
  out.valid_flag :: temp.valid_flag;
  out.* :: in.*;
end;
[Proof Join - criterias gueltig_von and gueltig_bis] type=reformat
  out::reformat(in) =
begin
  let date("YYYYMMDD") month_last_day =(date('YYYYMMDD'))datetime_add(in.monats_id,date_month_end(date_month(in.monats_id),date_year(in.monats_id)));
  let integer(4) valid_flag =if ((is_null(in.gueltig_von) or month_last_day > in.gueltig_von) 
and (is_null(in.gueltig_bis) or month_last_day <= in.gueltig_bis))
0
else
1;

  out.* :: in.*;
  out.rahmenvertrag_id :: if(valid_flag == 0)
in.rahmenvertrag_id;
  out.dwh_vertrag_id :: if(valid_flag == 0)
in.dwh_vertrag_id;
  out.dwh_gp_id :: if(valid_flag == 0)
in.dwh_gp_id;
  out.dwh_konto_id :: if(valid_flag == 0)
in.dwh_konto_id;
  out.dwh_tarifgr_id :: if(valid_flag == 0)
in.dwh_tarifgr_id;  /*NUMBER*/
  out.vo_kenn :: if(valid_flag == 0)
in.vo_kenn;
  out.zv_id :: if(valid_flag == 0)
in.zv_id;
  out.gueltig_von :: if(valid_flag == 0)
in.gueltig_von;
end;
[Reformat for insert "fact data"] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.rahmenvertrag :: in.rahmenvertrag_id;
end;
[Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur] type=rollup
  /*Das Ladedatum wird auf Seiten von Informatica beim
select via SYSDATE gesetzt. 
Da dies nur ein Datum zurï¿½ckgibt, 
wird hier now1() verwendet.*/
out::rollup(in) =
begin
  out.* :: in.*;
  out.rechpos_brutto_eur :: sum(in.rechpos_brutto_eur);
  out.rechpos_netto_eur :: sum(in.rechpos_netto_eur);
  out.rechpos_mwst_eur :: sum(in.rechpos_mwst_eur);
  out.ladedatum :: now1();
  out.typ :: if(((in.rech_leistung_id_carm == 'RABATT' && in.vertrags_id == 0) \|\| in.pooling == 'P'))
'T';
end;
[Reformat for insert "temporary data"] type=reformat
  out::reformat(in) =
begin
  let datetime("YYYYMMDDHH24MISS") mindate =(datetime('YYYYMMDDHH24MISS'))(string(14))'19000101000000';

  out.* :: in.*;
  out.bearbeitung_datum :: mindate;
end;
[Decode rpos_geschaeftsform_kenn] type=reformat
  /*Das Ladedatum wird auf Seiten von Informatica beim
select via SYSDATE gesetzt. 
Da dies nur ein Datum zurï¿½ckgibt, 
wird hier now1() verwendet.*/
out::reformat(in) =
begin
  out.* :: in.*;
  out.rpos_geschaftsform_kenn :: if(in.rpos_geschaftsform_kenn=='F')
   if(in.vas_kenn == 'P30002')
   'G'
   else
   in.rpos_geschaftsform_kenn
else
in.rpos_geschaftsform_kenn;
  out.ladedatum :: now1();
end;
[Scan - Ranking over gueltig_von, dwh_vertrag_id desc] type=scan
  type temporary_type = 
record
  integer(1) first_time;
  decimal('\|') rank;
  decimal('\|') rank_increase;
  string('\|') last_gueltig_von = NULL;
  string('\|') last_dwh_vertrag_id = NULL;
end /* Temporary variable*/;


/*This function may be optionally defined. 
Initialize temporary*/
temp::initialize(in) =
begin
  temp.first_time :: 1;
  temp.rank :: 0;
  temp.rank_increase :: 1;
  temp.last_gueltig_von :: "";
  temp.last_dwh_vertrag_id :: "";
end;



/*Do computation*/
temp::scan(temp, in) =
begin
  let decimal('\|') rank = 0;
  let decimal('\|') rank_increase = 0;

if (! temp.first_time && in.dwh_vertrag_id == temp.last_dwh_vertrag_id && 
                         in.gueltig_von == temp.last_gueltig_von)                          
  begin
     rank = temp.rank;
     rank_increase = temp.rank_increase + 1;
  end
  else
  begin
     rank = temp.rank + temp.rank_increase;
     rank_increase = 1;
  end

  temp.first_time :: 0;
  temp.rank :: rank;
  temp.rank_increase :: rank_increase;
  temp.last_gueltig_von :: in.gueltig_von;
  temp.last_dwh_vertrag_id :: in.dwh_vertrag_id;
end;



/*Create output record*/
out::finalize(temp, in) =
begin
  out.* :: in.*;
  out.rankindex :: temp.rank;
  out.monats_id :: (decimal("\\001"))(string("\\001"))in.monats_id;
  out.dwh_vertrag_id :: if(in.dwh_vertrag_id!='\\000')in.dwh_vertrag_id;
end;
[Filter out invalid data] type=reformat
  /*Im Kontext dwh_vertrag_id und gueltig_von muss
hier fï¿½r die korrekte Durchfï¿½hrung der Rankings 
im Fall von NULL der Wert \\000 (NUL) gesetzt werden.*/
out::reformat(in) =
begin
  let string("\\001") tmp_dwh_vertrag_id =string_concat('000000000000000',in.dwh_vertrag_id);

  out.* :: in.*;
  out.dwh_vertrag_id :: if(is_null(in.dwh_vertrag_id))
'\\000'
else
string_substring(tmp_dwh_vertrag_id,string_length(tmp_dwh_vertrag_id)-15,16);
  out.gueltig_von :: if(is_null(in.gueltig_von))
'\\000'
else
in.gueltig_von;
end;
[Scan - Mark valid historized datasets] type=scan
  type temporary_type = 
record
  integer(1) first_time;
  integer(1) valid_flag;
end /* Temporary variable*/;


/*This function may be optionally defined. 
Initialize temporary*/
temp::initialize(in) =
begin
  temp.first_time :: 1;
  temp.valid_flag :: 1;
end;



/*Do computation*/
temp::scan(temp, in) =
begin
  let integer(1) my_valid_flag = 1;

if(temp.first_time == 1)
    my_valid_flag = 0; 
  else 
  begin
     if (temp.first_time == 0 && !is_null(in.gueltig_von))
     my_valid_flag = 0;
     else
     my_valid_flag = 1;
  end

  temp.first_time :: 0;
  temp.valid_flag :: my_valid_flag;
end;



/*Create output record*/
out::finalize(temp, in) =
begin
  out.valid_flag :: temp.valid_flag;
  out.* :: in.*;
end;
[Proof Join-criteriase gueltig_von and gueltig_bis] type=reformat
  out::reformat(in) =
begin
  let date("YYYYMMDD") month_last_day =(date('YYYYMMDD')) datetime_add(in.monats_id,date_month_end(date_month(in.monats_id),date_year(in.monats_id)));
  let integer(4) valid_flag =if ((is_null(in.gueltig_von) or month_last_day > in.gueltig_von)
and (is_null(in.gueltig_bis) or month_last_day <= in.gueltig_bis))
0
else
1;

  out.* :: in.*;
  out.rahmenvertrag_id :: if(valid_flag == 0)
in.rahmenvertrag_id;
  out.dwh_vertrag_id :: if(valid_flag == 0)
in.dwh_vertrag_id;
  out.dwh_gp_id :: if(valid_flag == 0)
in.dwh_gp_id;
  out.dwh_konto_id :: if(valid_flag == 0)
in.dwh_konto_id;
  out.dwh_tarifgr_id :: if(valid_flag == 0)
in.dwh_tarifgr_id;  /*NUMBER*/
  out.vo_kenn :: if(valid_flag == 0)
in.vo_kenn;
  out.gueltig_von :: if(valid_flag == 0)
in.gueltig_von;
end;
[Filter out where rankindex != 1] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
end;
[Rollup- sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur, anz_leistungen, anz_tickets] type=rollup
  out::rollup(in) =
begin
  out.* :: in.*;
  out.rechpos_brutto_eur :: sum(in.rechpos_brutto_eur);
  out.rechpos_netto_eur :: sum(in.rechpos_netto_eur);
  out.rechpos_mwst_eur :: sum(in.rechpos_mwst_eur);
  out.anz_leistungen :: sum(in.anz_leistungen);
  out.anz_tickets :: sum(in.anz_tickets);
end;
[Reformat for insert "Factoring Gutschriften"] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rahmenvertrag :: in.rahmenvertrag_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
end;
[Reformat for insert "Factoring Rechnungen"] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
  out.rahmenvertrag :: in.rahmenvertrag_id;
end;
[Reformat for insert "Reselling"] type=reformat
  out::reformat(in) =
begin
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
  out.rahmenvertrag :: in.rahmenvertrag_id;
end;
[Reformat Enderecord for Processing] type=reformat
  out::reformat(in) =
begin
  out.kennzeichen :: in.kennzeichen;
  out.bemerkung :: in.bemerkung;
  out.stichtag :: in.stichtag;
  out.anzahl :: in.anzahl;
  out.inhalt :: in.inhalt;
  out.erstellt_am :: (string_index(in.erstellt_am, ";") == 0) ? in.erstellt_am : string_substring(in.erstellt_am, 1, string_length(in.erstellt_am)-1);
end;
[Read File] type=reformat
  type input_type = 
record
  string("\\n") data;
end /* Metadata for records read from input files*/;

filename::get_filename(in) =
begin
  filename :: if (string_index(in.filename, "") > 0) in.filename;
end;


out::reformat(read, filename) =
begin
  out.datensatz :: read.data;
end;
[Reformat for DB and Filter out where Kompl_Kennzeichen != L] type=reformat
  out::reformat(in) =
begin
  out.monats_id :: (string(6))(date("YYYYMM"))date_add_months((date("YYYYMM")) string_substring(in.stichtag,1,6),-1);
  out.abs_grp :: string_substring(in.bemerkung,10,5) ;
  out.dateiname :: in.bemerkung;
  out.rechnung_datum :: (date("YYYYMMDD")) in.stichtag;
  out.rechnungsteil :: (string(1))"P";
  out.ladedatum :: now();
end;
[Reformat fï¿½r testzwecke] type=reformat
  out::reformat(in) =
begin
  out.vertrags_id :: in.vertrags_id;
  out.monats_id :: in.monats_id;
end;

=== FILTERS ===
[Filter by Expression]
  rech_leistung_id_carm == "RABATT"
[Split Data]
  kennzeichen == "$\{BHB_Nutzdatensatzkennung\}"
[Filter by Expression]
  delete_flag == 1
[Select "Positionen auf Debitorenebene" (temporary Data)]
  typ == 'T'
[Select "Factoring Gutschriften"]
  rpos_geschaftsform_kenn == 'G'
[Select "Factoring Rechnungen"]
  rpos_geschaftsform_kenn == 'F'
[Select "Reselling"]
  rpos_geschaftsform_kenn == 'R'
[Split Metadata]
  kennzeichen == "$\{BHB_Endedatensatzkennung\}"

=== DB JOINS ===
[Join with DB]
  select_sql=select rechnung_id
from DWH$TA_F_RPOS_CARM
where rechnung_id = :rechnung_id
and rechnung_datum = :rechnung_datum
and standardvertrags_id = :standardvertrags_id
and vertrags_id = :vertrags_id 
and rech_leistung_id_carm = :rech_leistung_id_carm
  (see matching entry under TRANSFORMS for the in/query_result output-column mapping)
[Join with DB, Determine rows to be deleted]
  select_sql=select
rechnung_id,
rechnung_datum,
standardvertrags_id,
vertrags_id,
rech_leistung_id_carm
from
DWH$TA_F_RPOS_FACT_CARM
where
rechnung_id = :rechnung_id and
rechnung_datum = :rechnung_datum and
standardvertrags_id = :standardvertrags_id and
vertrags_id = :vertrags_id and
rech_leistung_id_carm = :rech_leistung_id_carm
  (see matching entry under TRANSFORMS for the in/query_result output-column mapping)
[Join with DB, Determine rows to be deleted]
  select_sql=select
rechnung_id,
rechnung_datum,
standardvertrags_id,
vertrags_id,
rech_leistung_id_carm
from
DWH$TA_F_RPOS_RESELLING_CARM
where
rechnung_id = :rechnung_id and
rechnung_datum = :rechnung_datum and
standardvertrags_id = :standardvertrags_id and
vertrags_id = :vertrags_id and
rech_leistung_id_carm = :rech_leistung_id_carm
  (see matching entry under TRANSFORMS for the in/query_result output-column mapping)
[Join with DB, Determine rows to be deleted]
  select_sql=select
rechnung_id,
rechnung_datum,
debitor_id
from
DWH$TA_T_RPOS_CARM
where
rechnung_id = :rechnung_id and
rechnung_datum = :rechnung_datum and
debitor_id = :debitor_id
  (see matching entry under TRANSFORMS for the in/query_result output-column mapping)
[Join with dwh$ta_c_vertrag]
  select_sql=select 
c.rahmenvertrag_id,
c.dwh_vertrag_id,
c.dwh_gp_id,
c.dwh_konto_id,
c.dwh_tarifgr_id,
c.vo_kenn,
c.zv_id,
c.gueltig_von,
c.gueltig_bis
from 
dwh$ta_c_vertrag c
where
c.vertrag_id_carmen (+) = :vertrags_id and
c.gueltig_bis >= to_date('20050401', 'YYYYMMDD')
  (see matching entry under TRANSFORMS for the in/query_result output-column mapping)

=== SORTS AND DEDUPS ===
[@@@1] type=sort
  keys=rechnung_datum; rechnung_id; standardvertrags_id; vertrags_id; debitor_id
[Dedup Sorted - vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm] type=dedup
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm
[Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id] type=dedup
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
[Dedup Sorted - vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id] type=dedup
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id
[Dedup Sorted - vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm] type=dedup
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm
[Sort by vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id
[Sort by vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm
[Sort within Groups - Sort over rech_leistung_id_carm] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm
[Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id] type=dedup
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
[Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id-1] type=dedup
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
[Dedup Sorted - vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id] type=dedup
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id
[Sort by vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id
[Sort by vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm
[Sort within Groups - Sort over rech_leistung_id_carm] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm
[Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id] type=dedup
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
[Dedup Sorted - vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id] type=dedup
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id
[Sort by vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id
[Sort by vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm
[Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
[Sort within Groups - Sort over rech_leistung_id_carm] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm
[Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id] type=dedup
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
[Dedup Sorted - vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id] type=dedup
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id
[Sort by vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id, debitor_id] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id
[Sort by vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm
[Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
[Sort within Groups - Sort over rech_leistung_id_carm] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm
[Dedup Sorted over rechnung_id; rechnung_datum; debitor_id] type=dedup
  keys=rechnung_id; rechnung_datum; debitor_id
[Dedup Sorted over rechnung_id; rechnung_datum; debitor_id-1] type=dedup
  keys=rechnung_id; rechnung_datum; debitor_id
[Dedup Sorted over rechnung_id; rechnung_datum; debitor_id-2] type=dedup
  keys=rechnung_id; rechnung_datum; debitor_id
[Dedup Sorted - vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id] type=dedup
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id
[Sort by rechnung_id; rechnung_datum; debitor_id] type=sort
  keys=rechnung_id; rechnung_datum; debitor_id
[Sort by rechnung_id; rechnung_datum; debitor_id-1] type=sort
  keys=rechnung_id; rechnung_datum; debitor_id
[Sort by rechnung_id; rechnung_datum; debitor_id-2] type=sort
  keys=rechnung_id; rechnung_datum; debitor_id
[Sort by rechnung_id; rechnung_datum; debitor_id-3] type=sort
  keys=rechnung_id; rechnung_datum; debitor_id
[Sort by vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm, debitor_id] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id
[Dedup Sorted] type=dedup
  keys=rechnung_datum; rechnung_id; vertrags_id; standardvertrags_id; rech_leistung_id_carm
[Sort] type=sort
  keys=rechnung_datum; rechnung_id; vertrags_id; standardvertrags_id; rech_leistung_id_carm; gueltig_von descending; dwh_vertrag_id descending
[Sort-1] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; dwh_vertrag_id descending; monats_id; debitor_id; abs_grp; dwh_gp_id; dwh_konto_id; dwh_tarifgr_id; vo_kenn; rahmenvertrag_id; zv_id; verkauftes_basisprodukt_id; rechnungvertrag_id; pooling
[Sort within Groups - Sort by dwh_vertrag_id; monats_id; debitor_id; abs_grp; dwh_gp_id; dwh_konto_id; dwh_tarifgr_id; vo_kenn; rahmenvertrag_id; zv_id; verkauftes_basisprodukt_id; rechnungvertrag_id; pooling] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; dwh_vertrag_id descending; monats_id; debitor_id; abs_grp; dwh_gp_id; dwh_konto_id; dwh_tarifgr_id; vo_kenn; rahmenvertrag_id; zv_id; verkauftes_basisprodukt_id; rechnungvertrag_id; pooling
[Sort within Groups - Sort by gueltig_von descending; dwh_vertrag_id descending;] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; gueltig_von alnum null descending; dwh_vertrag_id alnum null descending
[Dedup Sorted] type=dedup
  keys=rechnung_datum; rechnung_id; vertrags_id; standardvertrags_id; rech_leistung_id_carm
[Sort] type=sort
  keys=rechnung_datum; rechnung_id; vertrags_id; standardvertrags_id; rech_leistung_id_carm; gueltig_von descending; dwh_vertrag_id descending
[Sort within Groups- Sort by dwh_vertrag_id; prob_vertrag_id; monats_id; debitor_id; abs_grp; prob_provider_kenn; dwh_vertrag_id; dwh_gp_id; dwh_konto_id; vo_kenn; rahmenvertrag_id; dwh_tarifgr_id; rpos_geschaftsform_kenn; vas_kenn] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; dwh_vertrag_id descending; prob_vertrag_id; monats_id; debitor_id; abs_grp; prob_provider_kenn; dwh_gp_id; dwh_konto_id; vo_kenn; rahmenvertrag_id; dwh_tarifgr_id; rpos_geschaftsform_kenn; vas_kenn
[Sort within Groups - Sort by gueltig_von; dwh_vertrag_id descending;] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; gueltig_von alnum null; dwh_vertrag_id alnum null descending
[@@@1] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; dwh_vertrag_id descending; prob_vertrag_id; monats_id; debitor_id; abs_grp; prob_provider_kenn; dwh_gp_id; dwh_konto_id; vo_kenn; rahmenvertrag_id; dwh_tarifgr_id; rpos_geschaftsform_kenn; vas_kenn
[Sort within Groups - Sort by kontier_grp_id; monats_id; rechpos_brutto_eur; rechpos_netto_eur; rechpos_mwst_eur; abs_grp; pooling; rechnungvertrag_id; verkauftes_basisprodukt_id; gueltig_von descending] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id; kontier_grp_id; monats_id; rechpos_brutto_eur; rechpos_netto_eur; rechpos_mwst_eur; abs_grp; pooling; rechnungvertrag_id; verkauftes_basisprodukt_id; gueltig_von descending
[Sort within Groups - Sort by monats_id; rechpos_brutto_eur; rechpos_netto_eur; rechpos_mwst_eur; abs_grp; prob_vertrag_id; prob_provider_kenn; anz_leistungen; anz_tickets; rpos_geschaftsform_kenn; vas_kenn; kontier_grp_id; gueltig_von descending] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id; monats_id; rechpos_brutto_eur; rechpos_netto_eur; rechpos_mwst_eur; abs_grp; prob_vertrag_id; prob_provider_kenn; anz_leistungen; anz_tickets; rpos_geschaftsform_kenn; vas_kenn; kontier_grp_id; gueltig_von descending
[Sort by rechnung_id, rechnung_datum, standardvertrags_id, rech_leistung_id_carm, debitor_id] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id
[Sort by vertrag_id_carmen] type=sort
  keys=vertrag_id_carmen
[Sort within Groups - order by rechnung_id, rechnung_datum, standardvertrags_id, rech_leistung_id_carm, debitor_id] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id
[Sort by vertrags_id, rechnung_id, rechnung_datum, standardvertrags_id, rech_leistung_id_carm, debitor_id] type=sort
  keys=vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id

=== TARGETS ===
[DWH$TA_F_RPOS_FACT_CARM] kind=insert table_or_path=dwh_ta_f_rpos_fact_carm
[DWH$TA_T_RPOS_CARM] kind=insert table_or_path=dwh_ta_t_rpos_carm
[DWH$TA_F_RPOS_CARM] kind=insert table_or_path=dwh_ta_f_rpos_carm
[DWH$TA_F_GPOS_FACT_CARM] kind=insert table_or_path=dwh_ta_f_gpos_fact_carm
[DWH$TA_F_RPOS_RESELLING_CARM] kind=insert table_or_path=dwh_ta_f_rpos_reselling_carm
[Delete rows from DWH$TA_F_RPOS_CARM-2] kind=delete table_or_path=DWH$TA_F_RPOS_CARM
  DELETE FROM DWH$TA_F_RPOS_CARM
WHERE  rechnung_datum = :rechnung_datum
AND    rechnung_id = :rechnung_id
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
[Update DWH$TA_K_MELDUNGEN] kind=update table_or_path=dwh$ta_k_meldungen
  update dwh$ta_k_meldungen 
set anzahl_ds_eof = :anzahl
  , dateiname = :dateiname
  , enderecord_text = :inhalt
  , zusatzinfo = :bemerkung 
where entrynr = :eintragsnr
[Delete rows from DWH$TA_F_GPOS_FACT_CARM] kind=delete table_or_path=DWH$TA_F_GPOS_FACT_CARM
  DELETE FROM DWH$TA_F_GPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
[Delete rows from DWH$TA_F_RPOS_CARM] kind=delete table_or_path=DWH$TA_F_RPOS_CARM
  DELETE FROM DWH$TA_F_RPOS_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
[Delete rows from DWH$TA_F_RPOS_CARM-1] kind=delete table_or_path=DWH$TA_F_RPOS_CARM
  DELETE FROM DWH$TA_F_RPOS_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
[Delete rows from DWH$TA_F_RPOS_FACT_CARM] kind=delete table_or_path=DWH$TA_F_RPOS_FACT_CARM
  DELETE FROM DWH$TA_F_RPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
[Delete rows from DWH$TA_F_RPOS_RESELLING_CARM] kind=delete table_or_path=DWH$TA_F_RPOS_RESELLING_CARM
  DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
[Delete rows from DWH$TA_T_RPOS_CARM] kind=delete table_or_path=DWH$TA_T_RPOS_CARM
  DELETE FROM DWH$TA_T_RPOS_CARM
WHERE  debitor_id = :debitor_id
AND    rechnung_datum = :rechnung_datum
AND    rechnung_id = :rechnung_id
[Update / Insert DWH$TA_K_RECH_ABSGRP] kind=update table_or_path=DWH$TA_K_RECH_ABSGRP
  UPDATE DWH$TA_K_RECH_ABSGRP
SET   rechnung_datum = :rechnung_datum, 
      ladedatum = :ladedatum
WHERE  monats_id = :monats_id
AND    abs_grp = :abs_grp
AND    dateiname = :dateiname
AND    rechnungsteil = :rechnungsteil

=== EDGES (source-to-target wiring) ===
  Sort by vertrag_id_carmen --> Replicate
  Sort --> Dedup Sorted
  node_928 --> Determine rows to be deleted
  node_232 --> Sort within Groups - Sort over rech_leistung_id_carm
  Sort by rechnung_id; rechnung_datum; debitor_id-2 --> Determine rows to be deleted (incl. dedup of port 1)
  Scan - Mark valid historized datasets --> Filter out invalid data
  Dedup Sorted --> Sort-1
  Reformat for insert "fact data" --> node_1162
  Filter out where rankindex != 1 --> Sort within Groups - Sort by gueltig_von; dwh_vertrag_id descending;
  Sort within Groups - Sort by gueltig_von descending; dwh_vertrag_id descending; --> Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur
  Sort within Groups - Sort over rech_leistung_id_carm --> Determine rows to be deleted
  Sort within Groups - Sort by gueltig_von; dwh_vertrag_id descending; --> node_1426
  Reformat for delete --> Delete rows from DWH$TA_F_GPOS_FACT_CARM
  Dedup Sorted --> node_1566
  Sort by rechnung_id; rechnung_datum; debitor_id --> Dedup Sorted over rechnung_id; rechnung_datum; debitor_id
  node_374 --> Sort within Groups - Sort over rech_leistung_id_carm
  Filter out where rankindex != 1 --> Sort within Groups - Sort by gueltig_von descending; dwh_vertrag_id descending;
  node_25 --> node_1740
  node_1584 --> node_1324
  Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id --> Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id --> Determine rows to be deleted
  Join with DB --> Filter by Expression
  node_382 --> node_512
  Select "Factoring Gutschriften" --> Select "Reselling"
  node_1675 --> node_1620
  node_284 --> Sort within Groups - Sort over rech_leistung_id_carm
  node_732 --> Sort within Groups - Sort over rech_leistung_id_carm
  Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id --> Delete rows from DWH$TA_F_RPOS_CARM
  Filter by Expression --> node_426
  Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur-1 --> Gather
  Determine rows to be deleted --> Dedup Sorted over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Reformat for DB and Filter out where Kompl_Kennzeichen != L --> Update / Insert DWH$TA_K_RECH_ABSGRP
  node_842 --> Replicate
  Replicate --> node_357
  node_686 --> Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Merge --> Join with dwh$ta_c_vertrag-1
  Scan - Ranking over gueltig_von desc; dwh_vertrag_id desc --> Filter out where rankindex != 1
  Reformat Enderecord for Update --> Update DWH$TA_K_MELDUNGEN
  node_550 --> node_650
  Scan - Ranking over gueltig_von, dwh_vertrag_id desc --> Filter out where rankindex != 1
  Reformat for insert "Factoring Gutschriften" --> node_1449
  Select "Factoring Rechnungen" --> Reformat for insert "Factoring Rechnungen"
  node_426 --> Delete rows from DWH$TA_F_RPOS_CARM-1
  Reformat --> Delete rows from DWH$TA_F_RPOS_RESELLING_CARM
  node_1097 --> Sort by vertrag_id_carmen
  Read Filename --> Read File
  Join with dwh$ta_c_vertrag-1 --> Sort by vertrag_id_carmen
  Join with DB, Determine rows to be deleted --> Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Split Data --> Reformat Referencerecord
  node_338 --> node_284
  node_794 --> node_732
  Filter out where rpos_geschaeftsform_kenn != 'S' --> node_1584
  Join with DB, Determine rows to be deleted --> Sort by rechnung_id; rechnung_datum; debitor_id-3
  node_456 --> Sort within Groups - Sort over rech_leistung_id_carm
  Reformat for insert "temporary data" --> node_1086
  Replicate --> Reformat rechnung_datum to datetime for Delete
  Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur --> Select "Positionen auf Debitorenebene" (temporary Data)
  Format Enderecord --> Replicate Enderecord
  Sort by rechnung_id; rechnung_datum; debitor_id-1 --> Dedup Sorted over rechnung_id; rechnung_datum; debitor_id-1
  Reformat for DB --> Validate Records
  Reformat --> Delete rows from DWH$TA_T_RPOS_CARM
  Select "Positionen auf Debitorenebene" (temporary Data) --> Reformat for insert "fact data"
  Select "Positionen auf Debitorenebene" (temporary Data) --> Reformat for insert "temporary data"
  Sort by rechnung_id; rechnung_datum; debitor_id-3 --> Dedup Sorted over rechnung_id; rechnung_datum; debitor_id-2
  Proof Join-criteriase gueltig_von and gueltig_bis --> Replicate
  node_588 --> Sort within Groups - Sort over rech_leistung_id_carm
  node_1573 --> node_1149
  Redefine csv-file format --> Reformat for DB
  Replicate --> node_928
  Decode rpos_geschaeftsform_kenn --> node_1438
  Sort within Groups - Sort by gueltig_von descending; dwh_vertrag_id descending; --> Scan - Ranking over gueltig_von desc; dwh_vertrag_id desc
  Reformat Data --> Split Data
  node_192 --> Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur-1
  node_542 --> Sort over over vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id
  Dedup Sorted over rechnung_id; rechnung_datum; debitor_id-1 --> Determine rows to be deleted (incl. dedup of port 1)
  node_1576 --> Scan - Mark valid historized datasets
  Split Data --> Split Metadata
  Reformat for insert "Factoring Rechnungen" --> node_1053
  node_1426 --> Decode rpos_geschaeftsform_kenn
  node_694 --> node_794
  node_650 --> node_588
  Reformat for insert "Reselling" --> node_1459
  node_1740 --> Replicate
  Replicate --> Scan - Mark valid historized datasets
  node_207 --> Replicate
  node_1675 --> node_1746
  node_512 --> node_456
  Select "Factoring Rechnungen" --> Select "Factoring Gutschriften"
  Select "Reselling" --> Reformat for insert "Reselling"
  node_298 --> Determine rows to be deleted
  node_834 --> Sort by rechnung_id; rechnung_datum; debitor_id-1
  Select "Factoring Gutschriften" --> Reformat for insert "Factoring Gutschriften"
  Reformat rechnung_datum to datetime for Delete --> node_200
  replace ',' by '.' --> Redefine csv-file format
  Sort by vertrag_id_carmen --> Merge
  node_1620 --> Reformat for DB and Filter out where Kompl_Kennzeichen != L
  Replicate Enderecord --> Reformat Enderecord for Processing
  Join CSV-File with dwh$TA_C_VERTRAG --> Process Enderecord
  Determine rows to be deleted --> Sort by rechnung_id; rechnung_datum; debitor_id
  node_1746 --> node_25
  node_240 --> node_338
  Sort within Groups - Sort by gueltig_von; dwh_vertrag_id descending; --> Scan - Ranking over gueltig_von, dwh_vertrag_id desc
  Proof Join - criterias gueltig_von and gueltig_bis --> node_1576
  Replicate --> Filter out where rpos_geschaeftsform_kenn != 'S'
  Filter by Expression --> node_192
  Sort within Groups - Sort over rech_leistung_id_carm --> node_298
  Reformat fï¿½r testzwecke --> node_1750
  Reformat for delete --> Delete rows from DWH$TA_F_RPOS_FACT_CARM
  Read File --> Reformat Data


# TECHNICAL DESIGN DOCUMENT

## 1. GRAPH OVERVIEW
The Ab Initio graph **tmppqg6cj9n** processes telecommunication billing and invoice position records ("Rechnungsdaten") generated by the Carmen billing system. It filters, validates, and refines these billing streams by validating identifiers, aggregating monetary amounts, and enriching the positions with historical contract metadata loaded from the database contract reference table (`dwh$ta_c_vertrag`). Ultimately, the graph implements a clean transactional reload (Delete-then-Insert) pattern across several target tables categorised by billing type (Factoring invoices, Factoring credit notes, Reselling, and general billing structures), while logging metadata metrics back to control tables.

---

## 2. SOURCES
Below are the extracted source declarations exactly as retrieved from the graph database queries:

*   **Label:** `DWH$TA_F_RPOS_CARM`
    *   **Kind:** select
    *   **SQL:**
        ```sql
        select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_CARM
        ```
*   **Label:** `DWH$TA_F_RPOS_CARM-2`
    *   **Kind:** select
    *   **SQL:**
        ```sql
        select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_CARM
        ```
*   **Label:** `DWH$TA_F_RPOS_FACT_CARM`
    *   **Kind:** select
    *   **SQL:**
        ```sql
        select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_FACT_CARM
        ```
*   **Label:** `DWH$TA_F_RPOS_FACT_CARM - 2`
    *   **Kind:** select
    *   **SQL:**
        ```sql
        select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_FACT_CARM
        ```
*   **Label:** `DWH$TA_F_RPOS_RESELLING_CARM`
    *   **Kind:** select
    *   **SQL:**
        ```sql
        select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_RESELLING_CARM
        ```
*   **Label:** `DWH$TA_F_RPOS_RESELLING_CARM-1`
    *   **Kind:** select
    *   **SQL:**
        ```sql
        select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_RESELLING_CARM
        ```
*   **Label:** `dwh$ta_c_vertrag`
    *   **Kind:** select
    *   **SQL:**
        ```sql
        select 
        rahmenvertrag_id,
        vertrag_id_carmen,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von, 
        gueltig_bis
        from 
        dwh$ta_c_vertrag
        where 
        gueltig_bis >= to_date('20050401', 'YYYYMMDD') 
        and ABLOCAL(dwh$ta_c_vertrag)
        ```

---

## 3. TRANSFORMS

*   **Label:** `Reformat rechnung_datum to datetime for Delete`
    *   **Type:** `reformat`
    *   **Expression:**
        ```
        out::reformat(in) =
        begin
          out.* :: in.*;
        end;
        ```
    *   **Description:** Passes through all fields to establish input boundaries for the deletion-matching stream.

*   **Label:** `Validate Records`
    *   **Type:** `reformat`
    *   **Expression:**
        ```
        out::reformat(in) =
        begin
          out.monats_id :: if(!is_valid(in.monats_id)) force_error("Invalid data format in monats_id") else in.monats_id;
          out.rechnung_datum :: if(!is_valid(in.rechnung_datum)) force_error("Invalid data format in rechnung_datum") else in.rechnung_datum;
          out.standardvertrags_id :: if(!is_valid(in.standardvertrags_id)) force_error("Invalid data format in standardvertrags_id") else in.standardvertrags_id;
          out.vertrags_id :: if(!is_valid(in.vertrags_id)) force_error("Invalid data format in vertrags_id") else in.vertrags_id;
          out.rechpos_brutto_eur :: if(!is_valid(in.rechpos_brutto_eur)) force_error("Invalid data format in rechpos_brutto_eur") else in.rechpos_brutto_eur;
          out.rechpos_netto_eur :: if(!is_valid(in.rechpos_netto_eur)) force_error("Invalid data format in rechpos_netto_eur") else in.rechpos_netto_eur;
          out.rechpos_mwst_eur :: if(!is_valid(in.rechpos_mwst_eur)) force_error("Invalid data format in rechpos_mwst_eur") else in.rechpos_mwst_eur;
          out.* :: in.*;
        end;
        ```
    *   **Description:** Performs format validation on business-critical fields and triggers errors if nulls or malformed records exist.

*   **Label:** `Reformat for DB`
    *   **Type:** `reformat`
    *   **Expression:**
        ```
        out::reformat(in) =
        begin
          let integer(4) v_abs_grp_pos = 9;
          let integer(4) v_abs_grp_len = 5;
          let string("\001") tmp_abs_grp =string_substring(in.rechnung_id,v_abs_grp_pos, v_abs_grp_len);
          let string("\001") tmp_standardvertrags_id =string_lrtrim(in.standardvertrags_id);
          let string("\001") tmp_vertrags_id =string_lrtrim(in.vertrags_id);

          out.monats_id :: if(is_blank(in.monats_id)) force_error("Invalid Data in field monats_id") else (date("YYYYMM"))in.monats_id;
          out.debitor_id :: if(is_blank(in.debitor_id)) force_error("Invalid Data in field debitor_id") else string_lrtrim(in.debitor_id);
          out.rechnung_id :: if(is_blank(in.rechnung_id)) force_error("Invalid Data in field rechnung_id") else string_lrtrim(in.rechnung_id);
          out.rechnung_datum :: if(is_blank(in.rechnung_datum)) force_error("Invalid Data in field rechnung_datum") else (date("YYYYMMDD"))in.rechnung_datum;
          out.standardvertrags_id :: if(is_blank(in.standardvertrags_id)) force_error("Invalid Data in field standardvertrags_id") else if(tmp_standardvertrags_id != "#") string_lrtrim(tmp_standardvertrags_id);
          out.vertrags_id :: if(is_blank(in.vertrags_id)) force_error("Invalid Data in field vertrags_id") else if(tmp_vertrags_id != '#') string_lrtrim(tmp_vertrags_id);
          out.rech_leistung_id_carm :: if(is_blank(in.rech_leistung_id_carm)) force_error("Invalid Data in field rech_leistung_id_carm") else string_lrtrim(in.rech_leistung_id_carm);
          out.rechpos_brutto_eur :: if(is_blank(in.rechpos_brutto_eur)) force_error("Invalid Data in field rechpos_brutto_eur") else in.rechpos_brutto_eur;
          out.rechpos_netto_eur :: if(is_blank(in.rechpos_netto_eur)) force_error("Invalid Data in field rechpos_netto_eur") else in.rechpos_netto_eur;
          out.rechpos_mwst_eur :: if(is_blank(in.rechpos_mwst_eur)) force_error("Invalid Data in field rechpos_mwst_eur") else in.rechpos_mwst_eur;
          out.abs_grp :: if(!is_blank(tmp_abs_grp)) string_lrtrim(tmp_abs_grp);
          out.pooling :: if(!is_blank(in.pooling)) string_lrtrim(in.pooling);
          out.rechnungvertrag_id :: if(!is_blank(in.rechnungvertrag_id)) (decimal("\n"))string_lrtrim(in.rechnungvertrag_id);
          out.prob_vertrag_id :: if(!is_blank(in.prob_vertrag_id)) string_lrtrim(in.prob_vertrag_id);
          out.prob_provider_kenn :: if(!is_blank(in.prob_provider_kenn)) string_lrtrim(in.prob_provider_kenn);
          out.anz_leistungen :: if(!is_blank(in.anz_leistungen)) (decimal("\n")) string_lrtrim(in.anz_leistungen);
          out.anz_tickets :: if(!is_blank(in.anz_tickets)) (decimal("\n")) string_lrtrim(in.anz_tickets);
          out.rpos_geschaftsform_kenn :: if(!is_blank(in.rpos_geschaftsform_kenn)) in.rpos_geschaftsform_kenn;
          out.vas_kenn :: if(!is_blank(in.vas_kenn)) string_lrtrim(in.vas_kenn);
          out.verkauftes_basisprodukt_id :: if(!is_blank(in.kennung5)) string_lrtrim(in.kennung5);
        end;
        ```
    *   **Description:** Normalizes field types, extracts `abs_grp` from `rechnung_id`, trims string whitespaces, and enforces presence validations.

*   **Label:** `replace ',' by .`
    *   **Type:** `reformat`
    *   **Expression:**
        ```
        out::reformat(in) =
        begin
          out.kennzeichen :: in.kennzeichen;
          out.datensatz_rest :: string_replace(in.datensatz_rest, ',', '.');
        end;
        ```
    *   **Description:** Replaces commas with dots in the text body payload to facilitate clean parsing of decimals.

*   **Label:** `Reformat Referencerecord`
    *   **Type:** `reformat`
    *   **Expression:**
        ```
        out::reformat(in) =
        begin
          out.kennzeichen :: in.kennzeichen;
          out.datensatz_rest :: in.datensatz_rest;
        end;
        ```
    *   **Description:** Passthrough mapping for reference data streams.

*   **Label:** `Reformat Enderecord for Update`
    *   **Type:** `reformat`
    *   **Expression:**
        ```
        out::reformat(in) =
        begin
          out.dateiname  :: "${BHB_Dateiname}";
          out.eintragsnr :: ${BHB_Eintragsnr};
          out.bemerkung  :: in.bemerkung;
          out.anzahl     :: in.anzahl;
          out.inhalt     :: in.inhalt;
        end;
        ```
    *   **Description:** Prepares the footer record metrics to update job logs in control table `DWH$TA_K_MELDUNGEN`.

*   **Label:** `Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur-1`
    *   **Type:** `rollup`
    *   **Expression:**
        ```
        out::rollup(in) =
        begin
          out.* :: in.*;
          out.rechpos_brutto_eur :: sum(in.rechpos_brutto_eur);
          out.rechpos_netto_eur :: sum(in.rechpos_netto_eur);
          out.rechpos_mwst_eur :: sum(in.rechpos_mwst_eur);
        end;
        ```
    *   **Description:** Computes running summaries for billing components.

*   **Label:** `Determine rows to be deleted`
    *   **Type:** `join`
    *   **Expression:**
        ```
        out::join(in0, in1) =
        begin
          out.rechnung_id :: in0.rechnung_id;
          out.rechnung_datum :: in0.rechnung_datum;
          out.standardvertrags_id :: in0.standardvertrags_id;
          out.vertrags_id :: in0.vertrags_id;
          out.rech_leistung_id_carm :: in0.rech_leistung_id_carm;
          out.newline :: in0.newline;
        end;
        ```
    *   **Description:** Intersects processed streams with database records to detect matching rows slated for deletion.

*   **Label:** `Reformat for delete`
    *   **Type:** `reformat`
    *   **Expression:**
        ```
        out::reformat(in) =
        begin
          out.rechnung_id :: in.rechnung_id;
          out.rechnung_datum :: in.rechnung_datum;
          out.standardvertrags_id :: in.standardvertrags_id;
          out.vertrags_id :: in.vertrags_id;
          out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
        end;
        ```
    *   **Description:** Projects natural key identifiers required to identify and remove stale billing values.

*   **Label:** `Join with DB`
    *   **Type:** `join_with_db`
    *   **Expression:** See detailed SQL in Section 7.
    *   **Description:** Verifies whether a matching invoice record exists in target `DWH$TA_F_RPOS_CARM` to assign the `delete_flag`.

*   **Label:** `Join with DB, Determine rows to be deleted` (for FACT_CARM)
    *   **Type:** `join_with_db`
    *   **Expression:** See detailed SQL in Section 7.
    *   **Description:** Live database query to fetch target records matching input parameters from `DWH$TA_F_RPOS_FACT_CARM`.

*   **Label:** `Join with DB, Determine rows to be deleted` (for RESELLING)
    *   **Type:** `join_with_db`
    *   **Expression:** See detailed SQL in Section 7.
    *   **Description:** Live database query to fetch target records matching input parameters from `DWH$TA_F_RPOS_RESELLING_CARM`.

*   **Label:** `Join with DB, Determine rows to be deleted` (for RPOS_CARM)
    *   **Type:** `join_with_db`
    *   **Expression:** See detailed SQL in Section 7.
    *   **Description:** Live query targeting `DWH$TA_T_RPOS_CARM` to find deletable segments.

*   **Label:** `Join with dwh$ta_c_vertrag`
    *   **Type:** `join_with_db`
    *   **Expression:** See detailed SQL in Section 7.
    *   **Description:** Performs outer join enrichment with reference table `dwh$ta_c_vertrag` based on matching `vertrags_id` where `gueltig_bis` satisfies historical thresholds.

*   **Label:** `Scan - Ranking over gueltig_von desc; dwh_vertrag_id desc`
    *   **Type:** `scan` (Simulated via Dense Rank / Rank in Spark)
    *   **Expression:**
        Calculates rank groups descending on `gueltig_von` and `dwh_vertrag_id`.
    *   **Description:** Computes record sequence rankings to identify the most recent active contract interval.

*   **Label:** `Filter out invalid data`
    *   **Type:** `reformat`
    *   **Expression:**
        ```
        out::reformat(in) =
        begin
          let string("\001") tmp_dwh_vertrag_id = string_concat('000000000000000', in.dwh_vertrag_id);
          out.* :: in.*;
          out.dwh_vertrag_id :: if(is_null(in.dwh_vertrag_id)) '\000' else string_substring(tmp_dwh_vertrag_id, string_length(tmp_dwh_vertrag_id)-15, 16);
          out.gueltig_von :: if(is_null(in.gueltig_von)) '\000' else in.gueltig_von;
        end;
        ```
    *   **Description:** Imputes low-value NUL hex flags (`\000`) for missing database relationship values to secure sort predictability.

*   **Label:** `Scan - Mark valid historized datasets`
    *   **Type:** `scan`
    *   **Description:** Scans over grouped records to identify validity intervals across historical changes.

*   **Label:** `Proof Join - criterias gueltig_von and gueltig_bis`
    *   **Type:** `reformat`
    *   **Expression:**
        ```
        out::reformat(in) =
        begin
          let date("YYYYMMDD") month_last_day = (date('YYYYMMDD'))datetime_add(in.monats_id, date_month_end(date_month(in.monats_id), date_year(in.monats_id)));
          let integer(4) valid_flag = if ((is_null(in.gueltig_von) or month_last_day > in.gueltig_von) and (is_null(in.gueltig_bis) or month_last_day <= in.gueltig_bis)) 0 else 1;
          out.* :: in.*;
          out.rahmenvertrag_id :: if(valid_flag == 0) in.rahmenvertrag_id;
          out.dwh_vertrag_id :: if(valid_flag == 0) in.dwh_vertrag_id;
          out.dwh_gp_id :: if(valid_flag == 0) in.dwh_gp_id;
          out.dwh_konto_id :: if(valid_flag == 0) in.dwh_konto_id;
          out.dwh_tarifgr_id :: if(valid_flag == 0) in.dwh_tarifgr_id;
          out.vo_kenn :: if(valid_flag == 0) in.vo_kenn;
          out.zv_id :: if(valid_flag == 0) in.zv_id;
          out.gueltig_von :: if(valid_flag == 0) in.gueltig_von;
        end;
        ```
    *   **Description:** Determines if contract parameters remain valid relative to the billing month-end boundaries, nullifying variables if constraints fail.

*   **Label:** `Reformat for insert "fact data"`
    *   **Type:** `reformat`
    *   **Expression:**
        ```
        out::reformat(in) =
        begin
          out.* :: in.*;
          out.rahmenvertrag :: in.rahmenvertrag_id;
        end;
        ```
    *   **Description:** Maps resolved framework contract ids to target schema column variables.

*   **Label:** `Rollup - sum of rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur`
    *   **Type:** `rollup`
    *   **Expression:**
        ```
        out::rollup(in) =
        begin
          out.* :: in.*;
          out.rechpos_brutto_eur :: sum(in.rechpos_brutto_eur);
          out.rechpos_netto_eur :: sum(in.rechpos_netto_eur);
          out.rechpos_mwst_eur :: sum(in.rechpos_mwst_eur);
          out.ladedatum :: now1();
          out.typ :: if(((in.rech_leistung_id_carm == 'RABATT' && in.vertrags_id == 0) || in.pooling == 'P')) 'T';
        end;
        ```
    *   **Description:** Aggregates invoice values and assigns run datestamps alongside record category flags.

*   **Label:** `Decode rpos_geschaeftsform_kenn`
    *   **Type:** `reformat`
    *   **Expression:**
        ```
        out::reformat(in) =
        begin
          out.* :: in.*;
          out.rpos_geschaftsform_kenn :: if(in.rpos_geschaftsform_kenn=='F')
                                           if(in.vas_kenn == 'P30002') 'G'
                                           else in.rpos_geschaftsform_kenn
                                         else in.rpos_geschaftsform_kenn;
          out.ladedatum :: now1();
        end;
        ```
    *   **Description:** Overrides business models to 'G' (Factoring Credit Notes) if commercial code is 'F' paired with specific VAS value parameters.

---

## 4. IN-MEMORY LOOKUPS
*(None Extracted)*

---

## 5. FILTERS (select_expr)

*   **Label:** `Filter by Expression (RABATT)`
    *   **Expression:** `rech_leistung_id_carm == "RABATT"`
    *   **Effect:** Isolates specific billing rebate postings.

*   **Label:** `Split Data`
    *   **Expression:** `kennzeichen == "${BHB_Nutzdatensatzkennung}"`
    *   **Effect:** Routes standard business data payloads while excluding control markers.

*   **Label:** `Filter by Expression (Delete Flag)`
    *   **Expression:** `delete_flag == 1`
    *   **Effect:** Isolates database records identified as present for removal.

*   **Label:** `Select "Positionen auf Debitorenebene" (temporary Data)`
    *   **Expression:** `typ == 'T'`
    *   **Effect:** Selects grouped temporary records matching rebate or pooling identifiers.

*   **Label:** `Select "Factoring Gutschriften"`
    *   **Expression:** `rpos_geschaftsform_kenn == 'G'`
    *   **Effect:** Isolates Factoring Credit Notes.

*   **Label:** `Select "Factoring Rechnungen"`
    *   **Expression:** `rpos_geschaftsform_kenn == 'F'`
    *   **Effect:** Isolates standard Factoring Invoice items.

*   **Label:** `Select "Reselling"`
    *   **Expression:** `rpos_geschaftsform_kenn == 'R'`
    *   **Effect:** Filters out items tagged as Reselling business records.

*   **Label:** `Split Metadata`
    *   **Expression:** `kennzeichen == "${BHB_Endedatensatzkennung}"`
    *   **Effect:** Isolates standard trail processing logs from footer sequences.

---

## 6. OUTPUT TARGETS

### Paired Reload Operations (DELETE Executed BEFORE INSERT)

#### 1. Table `DWH$TA_F_RPOS_CARM`
*   **Kind:** DELETE + INSERT
*   **DELETE SQL:**
    ```sql
    DELETE FROM DWH$TA_F_RPOS_CARM
    WHERE  rechnung_id = :rechnung_id
    AND    rechnung_datum = :rechnung_datum
    AND    standardvertrags_id = :standardvertrags_id
    AND    vertrags_id = :vertrags_id
    ```
*   **INSERT:** Written directly to BigQuery destination table path matching physical schema of `dwh_ta_f_rpos_carm`.

#### 2. Table `DWH$TA_F_RPOS_FACT_CARM`
*   **Kind:** DELETE + INSERT
*   **DELETE SQL:**
    ```sql
    DELETE FROM DWH$TA_F_RPOS_FACT_CARM
    WHERE  rechnung_id = :rechnung_id
    AND    rechnung_datum = :rechnung_datum
    AND    standardvertrags_id = :standardvertrags_id
    AND    vertrags_id = :vertrags_id
    ```
*   **INSERT:** Appends validated records into target destination table `dwh_ta_f_rpos_fact_carm`.

#### 3. Table `DWH$TA_F_GPOS_FACT_CARM`
*   **Kind:** DELETE + INSERT
*   **DELETE SQL:**
    ```sql
    DELETE FROM DWH$TA_F_GPOS_FACT_CARM
    WHERE  rechnung_id = :rechnung_id
    AND    rechnung_datum = :rechnung_datum
    AND    standardvertrags_id = :standardvertrags_id
    AND    vertrags_id = :vertrags_id
    ```
*   **INSERT:** Appends aggregated positions into table `dwh_ta_f_gpos_fact_carm`.

#### 4. Table `DWH$TA_F_RPOS_RESELLING_CARM`
*   **Kind:** DELETE + INSERT
*   **DELETE SQL:**
    ```sql
    DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM
    WHERE  rechnung_id = :rechnung_id
    AND    rechnung_datum = :rechnung_datum
    AND    standardvertrags_id = :standardvertrags_id
    AND    vertrags_id = :vertrags_id
    ```
*   **INSERT:** Inserts matches into target `dwh_ta_f_rpos_reselling_carm`.

#### 5. Table `DWH$TA_T_RPOS_CARM`
*   **Kind:** DELETE + INSERT
*   **DELETE SQL:**
    ```sql
    DELETE FROM DWH$TA_T_RPOS_CARM
    WHERE  debitor_id = :debitor_id
    AND    rechnung_datum = :rechnung_datum
    AND    rechnung_id = :rechnung_id
    ```
*   **INSERT:** Appends temporary billing records into target table `dwh_ta_t_rpos_carm`.

---

### Standalone Metadata / Control Updates

#### 1. Table `DWH$TA_K_MELDUNGEN`
*   **Kind:** UPDATE
*   **SQL:**
    ```sql
    update dwh$ta_k_meldungen 
    set anzahl_ds_eof = :anzahl
      , dateiname = :dateiname
      , enderecord_text = :inhalt
      , zusatzinfo = :bemerkung 
    where entrynr = :eintragsnr
    ```

#### 2. Table `DWH$TA_K_RECH_ABSGRP`
*   **Kind:** MERGE / UPSERT (indicated as `Update / Insert DWH$TA_K_RECH_ABSGRP`)
*   **UPDATE/MERGE SQL:**
    ```sql
    UPDATE DWH$TA_K_RECH_ABSGRP
    SET   rechnung_datum = :rechnung_datum, 
          ladedatum = :ladedatum
    WHERE  monats_id = :monats_id
    AND    abs_grp = :abs_grp
    AND    dateiname = :dateiname
    AND    rechnungsteil = :rechnungsteil
    ```

---

## 7. DB JOINS

The following live-database lookup queries are executed by `Join With DB` components. 

*   **Label:** `Join with DB`
    *   **Query:**
        ```sql
        select rechnung_id
        from DWH$TA_F_RPOS_CARM
        where rechnung_id = :rechnung_id
        and rechnung_datum = :rechnung_datum
        and standardvertrags_id = :standardvertrags_id
        and vertrags_id = :vertrags_id 
        and rech_leistung_id_carm = :rech_leistung_id_carm
        ```
    *   **Mapping:**
        *   `delete_flag`: Assigned `1` if matched (`query_result.rechnung_id` is defined/not null), otherwise `0`.
        *   Passes through `in.*`.

*   **Label:** `Join with DB, Determine rows to be deleted` (for FACT_CARM)
    *   **Query:**
        ```sql
        select
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rech_leistung_id_carm
        from
        DWH$TA_F_RPOS_FACT_CARM
        where
        rechnung_id = :rechnung_id and
        rechnung_datum = :rechnung_datum and
        standardvertrags_id = :standardvertrags_id and
        vertrags_id = :vertrags_id and
        rech_leistung_id_carm = :rech_leistung_id_carm
        ```
    *   **Mapping:** Output fields `rechnung_id`, `rechnung_datum`, `standardvertrags_id`, `vertrags_id`, `rech_leistung_id_carm` are mapped from `query_result`.

*   **Label:** `Join with DB, Determine rows to be deleted` (for RESELLING)
    *   **Query:**
        ```sql
        select
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rech_leistung_id_carm
        from
        DWH$TA_F_RPOS_RESELLING_CARM
        where
        rechnung_id = :rechnung_id and
        rechnung_datum = :rechnung_datum and
        standardvertrags_id = :standardvertrags_id and
        vertrags_id = :vertrags_id and
        rech_leistung_id_carm = :rech_leistung_id_carm
        ```
    *   **Mapping:** Output fields mapped from corresponding matched query results.

*   **Label:** `Join with DB, Determine rows to be deleted` (for RPOS_CARM)
    *   **Query:**
        ```sql
        select
        rechnung_id,
        rechnung_datum,
        debitor_id
        from
        DWH$TA_T_RPOS_CARM
        where
        rechnung_id = :rechnung_id and
        rechnung_datum = :rechnung_datum and
        debitor_id = :debitor_id
        ```
    *   **Mapping:** Returns matched invoice context for target records.

*   **Label:** `Join with dwh$ta_c_vertrag`
    *   **Query:**
        ```sql
        select 
        c.rahmenvertrag_id,
        c.dwh_vertrag_id,
        c.dwh_gp_id,
        c.dwh_konto_id,
        c.dwh_tarifgr_id,
        c.vo_kenn,
        c.zv_id,
        c.gueltig_von,
        c.gueltig_bis
        from 
        dwh$ta_c_vertrag c
        where
        c.vertrag_id_carmen (+) = :vertrags_id and
        c.gueltig_bis >= to_date('20050401', 'YYYYMMDD')
        ```
    *   **Mapping:**
        *   Left outer join mapping of Carmen `vertrags_id` (`c.vertrag_id_carmen (+)`).
        *   Resolved database fields map directly into output structure. Pass-through variables are retained from the incoming input streams.

---

## 8. BUSINESS SUMMARY
1.  **Ingestion and Parsing:** Read raw structural flat-file streams, perform validation on decimal/date indicators, clean number signs, and extract the processing context `abs_grp` from invoice key segments.
2.  **Contract Referencing:** Enrich parsed billing positions against the database reference `dwh$ta_c_vertrag` (contract registry) to append valid GP, accounts, tariffs, and standard parameters.
3.  **Active Versioning Lookup:** Evaluate potential multiple histories using descending-order scanning techniques on `gueltig_von` to resolve the single rank sequence matching active billing intervals.
4.  **Transaction Categorization:** Categorize business records into Reselling ('R'), Factoring Invoices ('F'), and Factoring Credit Notes ('G') based on business codes and value-added service (VAS) rules.
5.  **Pre-Target Purge:** Identify existing database items intersecting incoming invoice indices, and trigger a targeted DELETE command to wipe existing occurrences in targets before re-inserting freshly parsed items.
6.  **Transactional Aggregation and Insert:** Execute mathematical summarization (summing MWST, Netto, and Brutto balances), append timestamp metrics, and insert records across target tables while updating processing statistics in control tracking systems.

---

# PYSPARK INTERMEDIATE PIPELINE OUTLINE

```python
# 1. Source ingestion from BigQuery Dataset environments
df_ta_f_rpos_carm = spark.read.format("bigquery").option("table", "BIGQUERY_SOURCE_DS.dwh_ta_f_rpos_carm").load()
df_ta_f_rpos_carm.createOrReplaceTempView("src_ta_f_rpos_carm")

df_ta_f_rpos_fact_carm = spark.read.format("bigquery").option("table", "BIGQUERY_SOURCE_DS.dwh_ta_f_rpos_fact_carm").load()
df_ta_f_rpos_fact_carm.createOrReplaceTempView("src_ta_f_rpos_fact_carm")

df_ta_f_rpos_reselling_carm = spark.read.format("bigquery").option("table", "BIGQUERY_SOURCE_DS.dwh_ta_f_rpos_reselling_carm").load()
df_ta_f_rpos_reselling_carm.createOrReplaceTempView("src_ta_f_rpos_reselling_carm")

df_ta_c_vertrag = spark.read.format("bigquery").option("table", "BIGQUERY_SOURCE_DS.dwh_ta_c_vertrag").load()
df_ta_c_vertrag.createOrReplaceTempView("src_ta_c_vertrag")

# Read input flat-file stream
df_raw_file = spark.read.format("text").load("hdfs_or_gcs_path_to_input_data")
df_raw_file.createOrReplaceTempView("vw_raw_file")

# 2. replace ',' by '.' to ensure robust decimal parsing
df_cleaned_payload = spark.sql("""
    SELECT 
        substring(value, 1, 3) AS kennzeichen,
        replace(substring(value, 4), ',', '.') AS datensatz_rest
    FROM vw_raw_file
""")
df_cleaned_payload.createOrReplaceTempView("vw_cleaned_payload")

# 3. Split business payloads from control trails
df_split_data = spark.sql("""
    SELECT * 
    FROM vw_cleaned_payload
    WHERE kennzeichen = '${BHB_Nutzdatensatzkennung}'
""")
df_split_data.createOrReplaceTempView("vw_split_data")

# 4. Map flat data structure (Simulation of schema mapping after splitting data stream)
# Note: Specific schema structures are mapped explicitly below based on Reformat for DB.
df_reformat_for_db = spark.sql("""
    SELECT
        CAST(to_date(substring(datensatz_rest, 1, 6), 'yyyyMM') AS STRING) AS monats_id,
        trim(substring(datensatz_rest, 7, 13)) AS debitor_id,
        trim(substring(datensatz_rest, 20, 14)) AS rechnung_id,
        to_date(substring(datensatz_rest, 34, 8), 'yyyyMMdd') AS rechnung_datum,
        CASE WHEN trim(substring(datensatz_rest, 42, 10)) != '#' THEN trim(substring(datensatz_rest, 42, 10)) ELSE NULL END AS standardvertrags_id,
        CASE WHEN trim(substring(datensatz_rest, 52, 10)) != '#' THEN trim(substring(datensatz_rest, 52, 10)) ELSE NULL END AS vertrags_id,
        trim(substring(datensatz_rest, 62, 9)) AS rech_leistung_id_carm,
        CAST(trim(substring(datensatz_rest, 71, 15)) AS DECIMAL(15,2)) AS rechpos_brutto_eur,
        CAST(trim(substring(datensatz_rest, 86, 15)) AS DECIMAL(15,2)) AS rechpos_netto_eur,
        CAST(trim(substring(datensatz_rest, 101, 15)) AS DECIMAL(15,2)) AS rechpos_mwst_eur,
        trim(substring(substring(datensatz_rest, 20, 14), 9, 5)) AS abs_grp,
        trim(substring(datensatz_rest, 116, 1)) AS pooling,
        CAST(trim(substring(datensatz_rest, 117, 10)) AS DECIMAL(18,0)) AS rechnungvertrag_id,
        trim(substring(datensatz_rest, 127, 10)) AS prob_vertrag_id,
        trim(substring(datensatz_rest, 137, 2)) AS prob_provider_kenn,
        CAST(trim(substring(datensatz_rest, 139, 10)) AS DECIMAL(18,0)) AS anz_leistungen,
        CAST(trim(substring(datensatz_rest, 149, 10)) AS DECIMAL(18,0)) AS anz_tickets,
        substring(datensatz_rest, 159, 1) AS rpos_geschaftsform_kenn,
        trim(substring(datensatz_rest, 160, 6)) AS vas_kenn,
        trim(substring(datensatz_rest, 166, 10)) AS verkauftes_basisprodukt_id
    FROM vw_split_data
""")
df_reformat_for_db.createOrReplaceTempView("vw_reformat_for_db")

# 5. Validate core inputs to enforce type constraints before processing
df_validated_records = spark.sql("""
    SELECT * 
    FROM vw_reformat_for_db
    WHERE monats_id IS NOT NULL 
      AND rechnung_datum IS NOT NULL 
      AND standardvertrags_id IS NOT NULL 
      AND vertrags_id IS NOT NULL
      AND rechpos_brutto_eur IS NOT NULL 
      AND rechpos_netto_eur IS NOT NULL 
      AND rechpos_mwst_eur IS NOT NULL
""")
df_validated_records.createOrReplaceTempView("vw_validated_records")

# 6. DB Lookup Enrichment against Carmen reference registry
df_contract_joined = spark.sql("""
    SELECT 
        v.*,
        c.rahmenvertrag_id,
        c.dwh_vertrag_id,
        c.dwh_gp_id,
        c.dwh_konto_id,
        c.dwh_tarifgr_id,
        c.vo_kenn,
        c.zv_id,
        c.gueltig_von,
        c.gueltig_bis
    FROM vw_validated_records v
    LEFT OUTER JOIN src_ta_c_vertrag c
      ON v.vertrags_id = c.vertrag_id_carmen
      AND c.gueltig_bis >= to_date('20050401', 'yyyyMMdd')
""")
df_contract_joined.createOrReplaceTempView("vw_contract_joined")

# 7. Impute Low-Value Indicators for sorting
df_imputed_sort_keys = spark.sql("""
    SELECT 
        t.*,
        coalesce(substring(concat('000000000000000', CAST(t.dwh_vertrag_id AS STRING)), -16, 16), '\\000') AS clean_dwh_vertrag_id,
        coalesce(CAST(t.gueltig_von AS STRING), '\\000') AS clean_gueltig_von
    FROM vw_contract_joined t
""")
df_imputed_sort_keys.createOrReplaceTempView("vw_imputed_sort_keys")

# 8. Scan Simulation using Rank Functions over descending historical keys
df_ranked_contracts = spark.sql("""
    SELECT 
        *,
        rank() OVER (
            PARTITION BY vertrags_id, rechnung_id, rechnung_datum, standardvertrags_id, rech_leistung_id_carm 
            ORDER BY clean_gueltig_von DESC, clean_dwh_vertrag_id DESC
        ) AS rankindex
    FROM vw_imputed_sort_keys
""")
df_ranked_contracts.createOrReplaceTempView("vw_ranked_contracts")

# 9. Filter out records where rankindex != 1
df_prime_ranks = spark.sql("""
    SELECT * 
    FROM vw_ranked_contracts 
    WHERE rankindex = 1
""")
df_prime_ranks.createOrReplaceTempView("vw_prime_ranks")

# 10. Proof Join - Evaluate and filter active month-end limits
df_proof_joins = spark.sql("""
    SELECT 
        p.*,
        CASE 
            WHEN (gueltig_von IS NULL OR last_day(to_date(monats_id, 'yyyyMM')) > gueltig_von)
             AND (gueltig_bis IS NULL OR last_day(to_date(monats_id, 'yyyyMM')) <= gueltig_bis)
            THEN 0 ELSE 1 
        END AS valid_flag
    FROM vw_prime_ranks p
""")
df_proof_joins.createOrReplaceTempView("vw_proof_joins")

# 11. Normalize frameworks depending on valid_flag
df_normalized_contracts = spark.sql("""
    SELECT 
        monats_id, debitor_id, rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id,
        rech_leistung_id_carm, rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur,
        abs_grp, pooling, rechnungvertrag_id, prob_vertrag_id, prob_provider_kenn,
        anz_leistungen, anz_tickets, rpos_geschaftsform_kenn, vas_kenn, verkauftes_basisprodukt_id,
        CASE WHEN valid_flag = 0 THEN rahmenvertrag_id ELSE NULL END AS rahmenvertrag_id,
        CASE WHEN valid_flag = 0 THEN dwh_vertrag_id ELSE NULL END AS dwh_vertrag_id,
        CASE WHEN valid_flag = 0 THEN dwh_gp_id ELSE NULL END AS dwh_gp_id,
        CASE WHEN valid_flag = 0 THEN dwh_konto_id ELSE NULL END AS dwh_konto_id,
        CASE WHEN valid_flag = 0 THEN dwh_tarifgr_id ELSE NULL END AS dwh_tarifgr_id,
        CASE WHEN valid_flag = 0 THEN vo_kenn ELSE NULL END AS vo_kenn,
        CASE WHEN valid_flag = 0 THEN zv_id ELSE NULL END AS zv_id,
        CASE WHEN valid_flag = 0 THEN gueltig_von ELSE NULL END AS gueltig_von
    FROM vw_proof_joins
""")
df_normalized_contracts.createOrReplaceTempView("vw_normalized_contracts")

# 12. Decode commercial forms and enrich run timestamps
df_decoded_positions = spark.sql("""
    SELECT 
        *,
        CASE 
            WHEN rpos_geschaftsform_kenn = 'F' AND vas_kenn = 'P30002' THEN 'G'
            ELSE rpos_geschaftsform_kenn 
        END AS clean_geschaftsform_kenn,
        current_timestamp() AS ladedatum
    FROM vw_normalized_contracts
""")
df_decoded_positions.createOrReplaceTempView("vw_decoded_positions")

# 13. Map Framework details mapping
df_mapped_inserts = spark.sql("""
    SELECT 
        *,
        rahmenvertrag_id AS rahmenvertrag
    FROM vw_decoded_positions
""")
df_mapped_inserts.createOrReplaceTempView("vw_mapped_inserts")

# 14. Separate Factoring Invoices
df_factoring_invoices = spark.sql("""
    SELECT * 
    FROM vw_mapped_inserts
    WHERE clean_geschaftsform_kenn = 'F'
""")
df_factoring_invoices.createOrReplaceTempView("vw_factoring_invoices")

# 15. Separate Factoring Credit Notes (Gutschriften)
df_factoring_credits = spark.sql("""
    SELECT * 
    FROM vw_mapped_inserts
    WHERE clean_geschaftsform_kenn = 'G'
""")
df_factoring_credits.createOrReplaceTempView("vw_factoring_credits")

# 16. Separate Reselling records
df_reselling_items = spark.sql("""
    SELECT * 
    FROM vw_mapped_inserts
    WHERE clean_geschaftsform_kenn = 'R'
""")
df_reselling_items.createOrReplaceTempView("vw_reselling_items")

# 17. Aggregation for GPOS (Aggregate standard invoices)
df_gpos_aggregation = spark.sql("""
    SELECT 
        vertrags_id, rechnung_id, rechnung_datum, standardvertrags_id,
        sum(rechpos_brutto_eur) AS rechpos_brutto_eur,
        sum(rechpos_netto_eur) AS rechpos_netto_eur,
        sum(rechpos_mwst_eur) AS rechpos_mwst_eur,
        current_timestamp() AS ladedatum,
        CASE WHEN (first(rech_leistung_id_carm) = 'RABATT' AND vertrags_id = 0) OR first(pooling) = 'P' THEN 'T' ELSE NULL END AS typ
    FROM vw_mapped_inserts
    GROUP BY vertrags_id, rechnung_id, rechnung_datum, standardvertrags_id
""")
df_gpos_aggregation.createOrReplaceTempView("vw_gpos_aggregation")

# 18. Temporary Billing Positions filter (Typ = 'T')
df_temp_billing = spark.sql("""
    SELECT *, CAST('1900-01-01 00:00:00' AS TIMESTAMP) AS bearbeitung_datum
    FROM vw_gpos_aggregation
    WHERE typ = 'T'
""")
df_temp_billing.createOrReplaceTempView("vw_temp_billing")

# 19. Run-Control Merge metadata aggregation (For ABSGRP status updating)
df_absgrp_upsert = spark.sql("""
    SELECT DISTINCT
        monats_id,
        abs_grp,
        '${BHB_Dateiname}' AS dateiname,
        'P' AS rechnungsteil,
        rechnung_datum,
        current_timestamp() AS ladedatum
    FROM vw_mapped_inserts
""")
df_absgrp_upsert.createOrReplaceTempView("vw_absgrp_upsert")

# 20. Parse Control Footer and Update Metadata Logs
df_split_metadata = spark.sql("""
    SELECT * 
    FROM vw_cleaned_payload
    WHERE kennzeichen = '${BHB_Endedatensatzkennung}'
""")
df_split_metadata.createOrReplaceTempView("vw_split_metadata")

# Note: Updates on DWH$TA_K_MELDUNGEN and DWH$TA_K_RECH_ABSGRP can be committed at the end.
```

---

# TARGET WRITE PATTERN (TRANSACTIONAL PRE-DELETE & RE-INSERT)

To maintain consistent data delivery and prevent primary-key/duplicate violations during historical restarts, execution order must strictly execute **DELETE operations before INSERT operations**.

```python
# Helper to execute pre-insert target cleansing
def delete_records_from_bq(target_table, matching_df, join_keys):
    # This handles execution of the target table purges prior to writing new positions
    pass

# ---- WRITE SEQUENCE ----

# 1. Purge and reload DWH$TA_F_RPOS_CARM
delete_keys_rpos = ["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id"]
# Extract distinct transaction keys to wipe
df_deletes_rpos = df_mapped_inserts.select(delete_keys_rpos).distinct()
delete_records_from_bq("dwh_ta_f_rpos_carm", df_deletes_rpos, delete_keys_rpos)

# INSERT freshly processed values
df_mapped_inserts.write.format("bigquery").mode("append").save("BIGQUERY_TARGET_DS.dwh_ta_f_rpos_carm")


# 2. Purge and reload DWH$TA_F_RPOS_FACT_CARM
df_deletes_fact = df_factoring_invoices.select(delete_keys_rpos).distinct()
delete_records_from_bq("dwh_ta_f_rpos_fact_carm", df_deletes_fact, delete_keys_rpos)

# INSERT processed Factoring Invoices
df_factoring_invoices.write.format("bigquery").mode("append").save("BIGQUERY_TARGET_DS.dwh_ta_f_rpos_fact_carm")


# 3. Purge and reload DWH$TA_F_GPOS_FACT_CARM
df_deletes_gpos = df_gpos_aggregation.select(delete_keys_rpos).distinct()
delete_records_from_bq("dwh_ta_f_gpos_fact_carm", df_deletes_gpos, delete_keys_rpos)

# INSERT aggregated GPOS Records
df_gpos_aggregation.write.format("bigquery").mode("append").save("BIGQUERY_TARGET_DS.dwh_ta_f_gpos_fact_carm")


# 4. Purge and reload DWH$TA_F_RPOS_RESELLING_CARM
df_deletes_resell = df_reselling_items.select(delete_keys_rpos).distinct()
delete_records_from_bq("dwh_ta_f_rpos_reselling_carm", df_deletes_resell, delete_keys_rpos)

# INSERT processed Reselling items
df_reselling_items.write.format("bigquery").mode("append").save("BIGQUERY_TARGET_DS.dwh_ta_f_rpos_reselling_carm")


# 5. Purge and reload DWH$TA_T_RPOS_CARM (Temporary Records)
delete_keys_temp = ["debitor_id", "rechnung_datum", "rechnung_id"]
df_deletes_temp = df_temp_billing.select(delete_keys_temp).distinct()
delete_records_from_bq("dwh_ta_t_rpos_carm", df_deletes_temp, delete_keys_temp)

# INSERT temporary billing positions
df_temp_billing.write.format("bigquery").mode("append").save("BIGQUERY_TARGET_DS.dwh_ta_t_rpos_carm")


# 6. Execute Metadata updates / upserts (DWH$TA_K_RECH_ABSGRP)
# Simulate Merge/Upsert Logic on BigQuery
# MERGE BIGQUERY_TARGET_DS.dwh_ta_k_rech_absgrp target 
# USING df_absgrp_upsert src ON target.monats_id = src.monats_id AND target.abs_grp = src.abs_grp...
df_absgrp_upsert.write.format("bigquery").mode("append").option("operation", "upsert").save("BIGQUERY_TARGET_DS.dwh_ta_k_rech_absgrp")
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py` | Convert the legacy Ab Initio graph logic into a Dataproc Serverless PySpark pipeline that digests flat files from GCS, validates fields, enriches data via reference table joins, and executes transactional target updates. |

---

# ADD CONTEXT THE MCP COULD NOT SEE

### Job Dependencies
- **Upstream:**
  - `Shared Files — abinitio_pyspark_linked_job/isccr/abinitio/bin` (already migrated & merged under PR: https://github.com/gurunathan-prodapt/pi-agents/pull/767). The pipeline execution environment references the shared module converted from `r_ai_start` to initialize running variables.

### Execution Order
The execution pipeline must maintain the step ordering determined by the legacy dependency graph:
1. **Trigger Task:** Orchestrated by the UC4 definition `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml`.
2. **Environment/Configuration Setup:** Extracts variables from `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` to parameterize runtime directories and date masks.
3. **Task Wrapper Execution:** Invokes `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh` (migrated to a Python operator) to call the main pipeline.
4. **PySpark Pipeline Run:** Executes the main conversion logic of the Ab Initio graph `map_rpos_carmen_import.mp` via the target file `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py`.

### External System Replacements
- **Database Storage:** Relational database storage on Oracle (queried using configurations like `DB_TNS_NAME_DWH`) maps to native Google Cloud BigQuery datasets.
- **File System:** Local SAN folder endpoints `$DW_DIR_IMP_SAP/crs/work/` and `$DW_DIR_IMP_SAP/crs/store/` are replaced by dedicated Google Cloud Storage (GCS) buckets mirroring these structural namespaces.

### Cross-File Dependencies
- **Shared Reference Table:** The pipeline queries the common table `dwh$ta_c_vertrag` to enrich standard incoming position contract fields.
- **Shared Common Module:** Execution relies on core script frameworks migrated from `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start` to bootstrap and log script metrics.

### Target File Plan
- **Target File:** `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py`
  - **Language:** PySpark
  - **Source File:** `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp`

### Environment-Specific Values
The environment configurations used by this process are mapped and sourced as follows:

#### 1. GLOBAL (Environment-Wide)
These keys identify environment-level endpoints and retain uniform values across all pipelines inside the target GCP environment:
- **GCP_PROJECT:** Identifies the target GCP Project ID. Sourced in PySpark using `os.environ.get("GCP_PROJECT")`.
- **GCS_BUCKET:** Represents the global GCS bucket used for raw files and outputs. Sourced in PySpark using `os.environ.get("GCS_BUCKET")`.
- **BQ_DATASET:** Identifies the BigQuery dataset where reference and final tables reside. Sourced in PySpark using `os.environ.get("BQ_DATASET")`.
- **BQ_LOCATION:** Explicit geographical location for BigQuery computations. Sourced in PySpark using `os.environ.get("BQ_LOCATION")`.

#### 2. JOB-SPECIFIC
These attributes represent settings unique to this specific import pipeline and are resolved via a job-level configuration object:
- **BHB_Quellverzeichnis:** Relative incoming path on GCS (mirrors `/Projects/TMD/processing/BHB/BD_PROC` or dynamic path `$DW_DIR_IMP_SAP/crs/work/`). 
- **BHB_Zielverzeichnis:** Relative archive path on GCS (mirrors `$DW_DIR_IMP_SAP/crs/store/`).
- **BHB_Dateimaske:** Standard file glob matching pattern (`CARMEN_B_*_pos.fix`).
- **BHB_Kopfdatensatzkennung:** Header record identifier (`H`).
- **BHB_Nutzdatensatzkennung:** Position record identifier (`P`).
- **BHB_Endedatensatzkennung:** Footer record identifier (`X`).
- **BHB_Eintragsnr:** Dynamic run sequence number passed as a command-line parameter.
- **BHB_Dateiname:** Dynamic input file name parsed during execution.

### Risks and Manual Steps
- **Data Integrity and Failure Handlers:** Core validation steps in the Ab Initio graph utilize `force_error()` with literal validation messages (e.g. `"Invalid data format in monats_id"`, `"Invalid Data in field monats_id"`, etc.). These error-raising statements must be translated into PySpark validation functions that handle logging character-for-character without translation, and bad records should be safely routed to quarantine zones to avoid breaking the execution flow.
- **Transactional Purging (DELETE before INSERT):** The targets are reloaded using key-based DELETE-then-INSERT pipelines. To prevent concurrent consumers from querying an empty or partially populated target table, these delete/insert write cycles must run within transactional sessions in BigQuery.
- **Rank Skew Performance:** The dense rank/scan processes partition on transactional keys (`vertrags_id`, `rechnung_id`, etc.) to isolate active contract entries. Large volumes of records sharing identical keys can cause data skew inside Spark executors; partition settings must be validated to protect against memory overload.

---

=== FILE: abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh ===
#! /bin/ksh
# Script generated by software licensed from Ab Initio Software Corporation.
# Use and disclosure are subject to Ab Initio confidentiality and license terms.
export AB_HOME;AB_HOME=${AB_HOME:-/appl/local/abinitio/abinitio}
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
export AB_AIR_HOME;AB_AIR_HOME=${AB_AIR_HOME:-/appl/local/abinitio/abinitio-V2-14}
unset GDE_EXECUTION

export AB_COMPATIBILITY;AB_COMPATIBILITY=2.14.59

# Deployed execution script for graph "map_rpos_carmen_import", compiled at Tuesday, February 27, 2007 09:33:49 using GDE version 1.14.16
export AB_JOB;AB_JOB=${AB_JOB_PREFIX:-""}map_rpos_carmen_import
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

export AB_GRAPH_NAME;AB_GRAPH_NAME=map_rpos_carmen_import

_AB_PROXY_DIR=map_rpos_carmen_import-ProxyDir-$$
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
         AB_GRAPH_SCRIPT_REPOSIT_TRACKING=false ${AB_HOME}/bin/run-and-reposit "${_AB_PROJECT_NAME}"'/mp/map_rpos_carmen_import.mp' "${_AB_PROJECT_NAME}" _abort "$0" "$@"
      else
         AB_GRAPH_SCRIPT_REPOSIT_TRACKING=false ${AB_HOME}/bin/run-and-reposit "${_AB_PROJECT_NAME}"'/mp/map_rpos_carmen_import.mp' "${_AB_PROJECT_NAME}" _abort "$0"
      fi
   else
      if [ $# -gt 0 ]; then
         AB_GRAPH_SCRIPT_REPOSIT_TRACKING=false ${AB_HOME}/bin/run-and-reposit "${_AB_PROJECT_NAME}"'/mp/map_rpos_carmen_import.mp' "${_AB_PROJECT_NAME}" "$0" "$@"
      else
         AB_GRAPH_SCRIPT_REPOSIT_TRACKING=false ${AB_HOME}/bin/run-and-reposit "${_AB_PROJECT_NAME}"'/mp/map_rpos_carmen_import.mp' "${_AB_PROJECT_NAME}" "$0"
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
   print -- Error evaluating: 'parameter DB_TNS_NAME_DWH of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_USER_DWH;DB_USER_DWH=${DB_USER_DWH:-$DB_USER_DWH}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_USER_DWH of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_PASSWD_DWH;DB_PASSWD_DWH=${DB_PASSWD_DWH:-$DB_PASSWD_DWH}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_PASSWD_DWH of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_TNS_NAME_CRS;DB_TNS_NAME_CRS=${DB_TNS_NAME_CRS:-$DB_TNS_NAME_CRS}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_TNS_NAME_CRS of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_USER_CRS;DB_USER_CRS=${DB_USER_CRS:-$DB_USER_CRS}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_USER_CRS of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_PASSWD_CRS;DB_PASSWD_CRS=${DB_PASSWD_CRS:-$DB_PASSWD_CRS}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_PASSWD_CRS of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_TNS_NAME_SGM;DB_TNS_NAME_SGM=${DB_TNS_NAME_SGM:-$DB_TNS_NAME_SGM}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_TNS_NAME_SGM of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_USER_SGM;DB_USER_SGM=${DB_USER_SGM:-$DB_USER_SGM}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_USER_SGM of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_PASSWD_SGM;DB_PASSWD_SGM=${DB_PASSWD_SGM:-$DB_PASSWD_SGM}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_PASSWD_SGM of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_TNS_NAME_CADS;DB_TNS_NAME_CADS=${DB_TNS_NAME_CADS:-$DB_TNS_NAME_CADS}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_TNS_NAME_CADS of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_USER_CADS;DB_USER_CADS=${DB_USER_CADS:-$DB_USER_CADS}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_USER_CADS of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_PASSWD_CADS;DB_PASSWD_CADS=${DB_PASSWD_CADS:-$DB_PASSWD_CADS}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_PASSWD_CADS of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_TNS_NAME_CACM;DB_TNS_NAME_CACM=${DB_TNS_NAME_CACM:-$DB_TNS_NAME_CACM}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_TNS_NAME_CACM of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_USER_CACM;DB_USER_CACM=${DB_USER_CACM:-$DB_USER_CACM}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_USER_CACM of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export DB_PASSWD_CACM;DB_PASSWD_CACM=${DB_PASSWD_CACM:-$DB_PASSWD_CACM}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter DB_PASSWD_CACM of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export comment_env1;comment_env1='####################################'
export comment_env2;comment_env2='# BHB Environment Settings'
export comment_env3;comment_env3='# (Framework Parameter)'
export comment_env4;comment_env4='####################################'
export BHB_Projektverzeichnis;BHB_Projektverzeichnis=${BHB_Projektverzeichnis:-$BHB_Projektverzeichnis}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Projektverzeichnis of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export BHB_Graph;BHB_Graph=${BHB_Graph:-$BHB_Graph}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Graph of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export BHB_Prozesstyp;BHB_Prozesstyp=${BHB_Prozesstyp:-$BHB_Prozesstyp}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Prozesstyp of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export BHB_Eintragsnr;BHB_Eintragsnr=${BHB_Eintragsnr:-$BHB_Eintragsnr}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Eintragsnr of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export BHB_Quellverzeichnis;BHB_Quellverzeichnis=${BHB_Quellverzeichnis:-$BHB_Quellverzeichnis}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Quellverzeichnis of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export BHB_Zielverzeichnis;BHB_Zielverzeichnis=${BHB_Zielverzeichnis:-$BHB_Zielverzeichnis}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Zielverzeichnis of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export BHB_Dateimaske;BHB_Dateimaske=${BHB_Dateimaske:-$BHB_Dateimaske}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Dateimaske of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export BHB_Kopfdatensatzkennung;BHB_Kopfdatensatzkennung=${BHB_Kopfdatensatzkennung:-$BHB_Kopfdatensatzkennung}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Kopfdatensatzkennung of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export BHB_Nutzdatensatzkennung;BHB_Nutzdatensatzkennung=${BHB_Nutzdatensatzkennung:-$BHB_Nutzdatensatzkennung}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Nutzdatensatzkennung of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export BHB_Endedatensatzkennung;BHB_Endedatensatzkennung=${BHB_Endedatensatzkennung:-$BHB_Endedatensatzkennung}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Endedatensatzkennung of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export BHB_Dateiname;BHB_Dateiname=${BHB_Dateiname:-$BHB_Dateiname}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter BHB_Dateiname of map_rpos_carmen_import', interpretation 'shell'
   exit $mpjret
fi
export comment_loc_1;comment_loc_1='####################################'
export comment_loc_2;comment_loc_2='# BHB Local Settings'
export comment_loc_3;comment_loc_3='# (Special Parameter)'
export comment_loc_4;comment_loc_4='####################################'
. ./${_AB_PROXY_DIR}/GDE-Parameters

#+Script Start+  ==================== Edits in this section are preserved.
export NLS_NUMERIC_CHARACTERS=". ";

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
mv "${_AB_PROXY_DIR}" "${AB_JOB}"'-map_rpos_carmen_import-ProxyDir'
_AB_PROXY_DIR="${AB_JOB}"'-map_rpos_carmen_import-ProxyDir'
print -r -- 'out::join(in0, in1) =
begin
  out.rechnung_id :: in0.rechnung_id;
  out.rechnung_datum :: in0.rechnung_datum;
  out.standardvertrags_id :: in0.standardvertrags_id;
  out.vertrags_id :: in0.vertrags_id;
  out.rech_leistung_id_carm :: in0.rech_leistung_id_carm;
  out.newline :: in0.newline;  /*VARCHAR2(13) NOT NULL*/
end;' > "${_AB_PROXY_DIR}"'/Determine_rows_to_be_deleted-2.xfr'
print -r -- 'out::join(in0, in1) =
begin
  out.rechnung_id :: in0.rechnung_id;
  out.rechnung_datum :: in0.rechnung_datum;
  out.standardvertrags_id :: in0.standardvertrags_id;  /*VARCHAR2(13) NOT NULL*/
  out.vertrags_id :: in0.vertrags_id;
  out.rech_leistung_id_carm :: in0.rech_leistung_id_carm;
  out.newline :: in0.newline;
end;' > "${_AB_PROXY_DIR}"'/Determine_rows_to_be_deleted-3.xfr'
print -r -- 'DELETE FROM DWH$TA_F_RPOS_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id' > "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_F_RPOS_CARM-4.sql'
print -r -- 'type query_result_type = 
record
  string(unsigned integer(2)) rechnung_id; /* VARCHAR2(14) NOT NULL*/
end /* Generated type from select statement*/;

out::join_with_db(in, query_result) =
begin
  out.* :: in.*;
  out.delete_flag :: if(is_defined(query_result.rechnung_id))
 1
else
 0;
end;' > "${_AB_PROXY_DIR}"'/Join_with_DB-6.xfr'
print -r -- 'type query_result_type = 
record
  string(unsigned integer(1)) rechnung_id; /* VARCHAR2(14) NOT NULL*/
  datetime("YYYYMMDDHH24MISS") rechnung_datum; /* DATE NOT NULL*/
  decimal(11) standardvertrags_id; /* NUMBER(10) NOT NULL*/
  decimal(11) vertrags_id; /* NUMBER(10) NOT NULL*/
  string(unsigned integer(1)) rech_leistung_id_carm; /* VARCHAR2(9) NOT NULL*/
end /* Generated type from select statement*/;

out::join_with_db(in, query_result) =
begin
  out.rechnung_id :: query_result.rechnung_id;
  out.rechnung_datum :: query_result.rechnung_datum;
  out.standardvertrags_id :: query_result.standardvertrags_id;
  out.vertrags_id :: query_result.vertrags_id;
  out.rech_leistung_id_carm :: query_result.rech_leistung_id_carm;
end;' > "${_AB_PROXY_DIR}"'/Join_with_DB_Determine_rows_to_be_deleted-7.xfr'
print -r -- 'out::join(in0, in1) =
begin
  out.rechnung_id :: in0.rechnung_id;
  out.rechnung_datum :: in0.rechnung_datum;
  out.debitor_id :: in0.debitor_id;
  out.newline :: in0.newline;
end;' > "${_AB_PROXY_DIR}"'/Determine_rows_to_be_deleted-8.xfr'
print -r -- 'type query_result_type = 
record
  string(unsigned integer(1)) rechnung_id; /* VARCHAR2(14) NOT NULL*/
  datetime("YYYYMMDDHH24MISS") rechnung_datum; /* DATE NOT NULL*/
  string(unsigned integer(1)) debitor_id; /* VARCHAR2(13) NOT NULL*/
end /* Generated type from select statement*/;

out::join_with_db(in, query_result) =
begin
  out.rechnung_id :: query_result.rechnung_id;
  out.rechnung_datum :: query_result.rechnung_datum;
  out.debitor_id :: query_result.debitor_id;
end;' > "${_AB_PROXY_DIR}"'/Join_with_DB_Determine_rows_to_be_deleted-9.xfr'
print -r -- '/*Da f�r die Tabelle DWH$TA_C_VERTRAG
eine Basisschicht existiert d�rfte es
hier nie der Fall sein, dass gueltig_von
und gueltig_bis NULL sind.*/
out::join(in0, in1) =
begin
  out.* :: in0.*;
  out.rahmenvertrag_id :: in1.rahmenvertrag_id;
  out.dwh_vertrag_id :: in1.dwh_vertrag_id;
  out.dwh_gp_id :: in1.dwh_gp_id;
  out.dwh_konto_id :: in1.dwh_konto_id;
  out.dwh_tarifgr_id :: in1.dwh_tarifgr_id;
  out.vo_kenn :: in1.vo_kenn;
  out.zv_id :: in1.zv_id;
  out.gueltig_von :1: in1.gueltig_von;
  out.gueltig_bis :: in1.gueltig_bis;
end;' > "${_AB_PROXY_DIR}"'/Join_CSV_File_with_dwh_TA_C_VERTRAG-10.xfr'
print -r -- 'type query_result_type = 
record
  string(unsigned integer(1)) rahmenvertrag_id = NULL; /* VARCHAR2(10)*/
  decimal(17) dwh_vertrag_id; /* NUMBER(16) NOT NULL*/
  decimal(17) dwh_gp_id = NULL; /* NUMBER(16)*/
  decimal(17) dwh_konto_id = NULL; /* NUMBER(16)*/
  decimal(100) dwh_tarifgr_id = NULL; /* NUMBER*/
  string(5) vo_kenn = NULL; /* CHAR(5)*/
  string(unsigned integer(1)) zv_id = NULL; /* VARCHAR2(10)*/
  datetime("YYYYMMDDHH24MISS") gueltig_von; /* DATE NOT NULL*/
  datetime("YYYYMMDDHH24MISS") gueltig_bis = NULL; /* DATE*/
end /* Generated type from select statement*/;

out::join_with_db(in, query_result) =
begin
  out.* :: in.*;
  out.rahmenvertrag_id :: query_result.rahmenvertrag_id;
  out.dwh_vertrag_id :: query_result.dwh_vertrag_id;
  out.dwh_gp_id :: query_result.dwh_gp_id;
  out.dwh_konto_id :: query_result.dwh_konto_id;
  out.dwh_tarifgr_id :: query_result.dwh_tarifgr_id;
  out.vo_kenn :: query_result.vo_kenn;
  out.zv_id :: query_result.zv_id;
  out.gueltig_von :: query_result.gueltig_von;
  out.gueltig_bis :: query_result.gueltig_bis;
end;' > "${_AB_PROXY_DIR}"'/Join_with_dwh_ta_c_vertrag-11.xfr'
print -r -- '/* DML Generated for SQL: select 
rahmenvertrag_id,
vertrag_id_carmen,
dwh_vertrag_id,
dwh_gp_id,
dwh_konto_id,
dwh_tarifgr_id,
vo_kenn,
zv_id,
gueltig_von, 
gueltig_bis
from 
dwh$ta_c_vertrag
where 
gueltig_bis >= to_date('"'"'20050401'"'"', '"'"'YYYYMMDD'"'"') 
and 1 = 1
 * On: Sun Nov 19 17:26:10 2006

 */
record
  string("\x01", maximum_length=10) rahmenvertrag_id = NULL(""); /* VARCHAR2(10)*/
  decimal("\x01", maximum_length=102) vertrag_id_carmen = NULL(""); /* NUMBER*/
  decimal("\x01", maximum_length=19) dwh_vertrag_id; /* NUMBER(16) NOT NULL*/
  decimal("\x01", maximum_length=19) dwh_gp_id = NULL(""); /* NUMBER(16)*/
  decimal("\x01", maximum_length=19) dwh_konto_id = NULL(""); /* NUMBER(16)*/
  decimal("\x01", maximum_length=102) dwh_tarifgr_id = NULL(""); /* NUMBER*/
  string("\x01", maximum_length=5) vo_kenn = NULL(""); /* CHAR(5)*/
  string("\x01", maximum_length=10) zv_id = NULL(""); /* VARCHAR2(10)*/
  datetime("YYYYMMDDHH24MISS")("\x01") gueltig_von; /* DATE NOT NULL*/
  datetime("YYYYMMDDHH24MISS")("\x01") gueltig_bis = NULL(""); /* DATE*/
  string(1) newline = "\n";
end' > "${_AB_PROXY_DIR}"'/dwh_ta_c_vertrag-12.dml'
print -r -- 'record
  string("\n") filename;
end;' > "${_AB_PROXY_DIR}"'/Read_File-13.dml'
print -r -- 'type input_type = 
record
  string("\n") data;
end /* Metadata for records read from input files*/;

filename::get_filename(in) =
begin
  filename :: if (string_index(in.filename, "") > 0) in.filename;
end;


out::reformat(read, filename) =
begin
  out.datensatz :: read.data;
end;' > "${_AB_PROXY_DIR}"'/Read_File-14.xfr'
print -r -- 'record
  string("\n") datensatz;
end;' > "${_AB_PROXY_DIR}"'/Read_File-15.dml'
print -r -- 'record
  string(1) kennzeichen;
  string("\n") datensatz_rest;
end;' > "${_AB_PROXY_DIR}"'/Reformat_Data-16.dml'
print -r -- 'out::reformat(in) =
begin
  out.kennzeichen :: in.kennzeichen;
  out.datensatz_rest :: in.datensatz_rest;
end;' > "${_AB_PROXY_DIR}"'/Reformat_Referencerecord-17.xfr'
print -r -- 'out::reformat(in) =
begin
  out.kennzeichen :: in.kennzeichen;
  out.datensatz_rest :: string_replace(in.datensatz_rest, '"'"','"'"', '"'"'.'"'"');
end;' > "${_AB_PROXY_DIR}"'/replace_by_-18.xfr'
print -r -- '/*

*/
out::reformat(in) =
begin
  let integer(4) v_abs_grp_pos = 9;
  let integer(4) v_abs_grp_len = 5;
  let string("\001") tmp_abs_grp =string_substring(in.rechnung_id,v_abs_grp_pos, v_abs_grp_len);
  let string("\001") tmp_standardvertrags_id =string_lrtrim(in.standardvertrags_id);
  let string("\001") tmp_vertrags_id =string_lrtrim(in.vertrags_id);

  out.monats_id :: if(is_blank(in.monats_id))
force_error("Invalid Data in field monats_id")
else
(date("YYYYMM"))in.monats_id;
  out.debitor_id :: if(is_blank(in.debitor_id))
force_error("Invalid Data in field debitor_id")
else 
string_lrtrim(in.debitor_id);
  out.rechnung_id :: if(is_blank(in.rechnung_id))
force_error("Invalid Data in field rechnung_id")
else 
string_lrtrim(in.rechnung_id);
  out.rechnung_datum :: if(is_blank(in.rechnung_datum))
force_error("Invalid Data in field rechnung_datum")
else 
(date("YYYYMMDD"))in.rechnung_datum;
  out.standardvertrags_id :: if(is_blank(in.standardvertrags_id)) force_error("Invalid Data in field standardvertrags_id") else
if(tmp_standardvertrags_id != "#") string_lrtrim(tmp_standardvertrags_id);
  out.vertrags_id :: if(is_blank(in.vertrags_id)) force_error("Invalid Data in field vertrags_id") else 
if(tmp_vertrags_id != '"'"'#'"'"') string_lrtrim(tmp_vertrags_id);
  out.rech_leistung_id_carm :: if(is_blank(in.rech_leistung_id_carm))
force_error("Invalid Data in field rech_leistung_id_carm")
else 
string_lrtrim(in.rech_leistung_id_carm);
  out.rechpos_brutto_eur :: if(is_blank(in.rechpos_brutto_eur))
force_error("Invalid Data in field rechpos_brutto_eur")
else 
in.rechpos_brutto_eur;
  out.rechpos_netto_eur :: if(is_blank(in.rechpos_netto_eur))
force_error("Invalid Data in field rechpos_netto_eur")
else 
in.rechpos_netto_eur;
  out.rechpos_mwst_eur :: if(is_blank(in.rechpos_mwst_eur))
force_error("Invalid Data in field rechpos_mwst_eur")
else 
in.rechpos_mwst_eur;
  out.abs_grp :: if(!is_blank(tmp_abs_grp))
string_lrtrim(tmp_abs_grp);
  out.pooling :: if(!is_blank(in.pooling))
string_lrtrim(in.pooling);
  out.rechnungvertrag_id :: if(!is_blank(in.rechnungvertrag_id))
(decimal("\n"))string_lrtrim(in.rechnungvertrag_id);
  out.prob_vertrag_id :: if(!is_blank(in.prob_vertrag_id))
string_lrtrim(in.prob_vertrag_id);
  out.prob_provider_kenn :: if(!is_blank(in.prob_provider_kenn))
string_lrtrim(in.prob_provider_kenn);
  out.anz_leistungen :: if(!is_blank(in.anz_leistungen))
(decimal("\n")) string_lrtrim(in.anz_leistungen);
  out.anz_tickets :: if(!is_blank(in.anz_tickets))
(decimal("\n")) string_lrtrim(in.anz_tickets);
  out.rpos_geschaftsform_kenn :: if(!is_blank(in.rpos_geschaftsform_kenn))
in.rpos_geschaftsform_kenn;
  out.vas_kenn :: if(!is_blank(in.vas_kenn))
string_lrtrim(in.vas_kenn);
  out.verkauftes_basisprodukt_id :: if(!is_blank(in.kennung5))
string_lrtrim(in.kennung5);
end;' > "${_AB_PROXY_DIR}"'/Reformat_for_DB-20.xfr'
print -r -- 'record
  date("YYYYMM")("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp = "#";
  string("\001") pooling = '"'"'#'"'"';
  decimal("\001") rechnungvertrag_id = 0;
  string("\001") prob_vertrag_id = '"'"'#'"'"';
  string("\001") prob_provider_kenn = '"'"'#'"'"';
  decimal("\001") anz_leistungen = 0;
  decimal("\001") anz_tickets = 0;
  string("\001") rpos_geschaftsform_kenn = '"'"'#'"'"';
  string("\001") vas_kenn = '"'"'#'"'"';
  decimal("\001") verkauftes_basisprodukt_id = 0;
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Reformat_for_DB-21.dml'
print -r -- 'out::reformat(in) =
begin
  out.monats_id :: if(!is_valid(in.monats_id))
force_error("Invalid data format in monats_id")
else
in.monats_id;
  out.rechnung_datum :: if(!is_valid(in.rechnung_datum))
force_error("Invalid data format in rechnung_datum")
else
in.rechnung_datum;
  out.standardvertrags_id :: if(!is_valid(in.standardvertrags_id))
force_error("Invalid data format in standardvertrags_id")
else
in.standardvertrags_id;
  out.vertrags_id :: if(!is_valid(in.vertrags_id))
force_error("Invalid data format in vertrags_id")
else
in.vertrags_id;
  out.rechpos_brutto_eur :: if(!is_valid(in.rechpos_brutto_eur))
force_error("Invalid data format in rechpos_brutto_eur")
else
in.rechpos_brutto_eur;
  out.rechpos_netto_eur :: if(!is_valid(in.rechpos_netto_eur))
force_error("Invalid data format in rechpos_netto_eur")
else
in.rechpos_netto_eur;
  out.rechpos_mwst_eur :: if(!is_valid(in.rechpos_mwst_eur))
force_error("Invalid data format in rechpos_mwst_eur")
else
in.rechpos_mwst_eur;
  out.* :: in.*;
end;' > "${_AB_PROXY_DIR}"'/Validate_Records-22.xfr'
print -r -- 'record
/* Obwohl monats_id sp�ter als decimal
in die Datenbank geschrieben wird, findet
hier eine Konvertierung in ein Date statt,
da f�r eine sp�tere Pr�fung ein
korrekter Datumswert gesetzt sein muss.
*/
  date("YYYYMM")("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp;
  string("\001") pooling;
  decimal("\001") rechnungvertrag_id;
  string("\001") prob_vertrag_id;
  string("\001") prob_provider_kenn;
  decimal("\001") anz_leistungen;
  decimal("\001") anz_tickets;
  string("\001") rpos_geschaftsform_kenn;
  string("\001") vas_kenn;
  decimal("\001") verkauftes_basisprodukt_id = NULL("");
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Validate_Records-23.dml'
print -r -- '/*Das Ladedatum wird auf Seiten von Informatica beim
select via SYSDATE gesetzt. 
Da dies nur ein Datum zur�ckgibt, 
wird hier now1() verwendet.*/
out::rollup(in) =
begin
  out.* :: in.*;
  out.rechpos_brutto_eur :: sum(in.rechpos_brutto_eur);
  out.rechpos_netto_eur :: sum(in.rechpos_netto_eur);
  out.rechpos_mwst_eur :: sum(in.rechpos_mwst_eur);
end;' > "${_AB_PROXY_DIR}"'/Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_1-24.xfr'
print -r -- 'out::join(in0, in1) =
begin
  out.* :1: in0.*;
  out.* :: in1.*;
  out.dwh_vertrag_id :: if(!is_null(in1.dwh_vertrag_id))
 in1.dwh_vertrag_id;
  out.dwh_gp_id :: if(!is_null(in1.dwh_gp_id))
 in1.dwh_gp_id;
  out.dwh_konto_id :: if(!is_null(in1.dwh_konto_id))
 in1.dwh_konto_id;
  out.dwh_tarifgr_id :: if (!is_null(in1.dwh_tarifgr_id)) 
 in1.dwh_tarifgr_id;
  out.vo_kenn :: if(!is_null(in1.vo_kenn))
 in1.vo_kenn;
  out.zv_id :: if(!is_null(in1.zv_id))
 in1.zv_id;
end;' > "${_AB_PROXY_DIR}"'/Join_with_dwh_ta_c_vertrag_1-25.xfr'
print -r -- 'record
  date("YYYYMM")("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp;
  string("\001") pooling;
  decimal("\001") rechnungvertrag_id;
  string("\001") prob_vertrag_id;
  string("\001") prob_provider_kenn;
  decimal("\001") anz_leistungen;
  decimal("\001") anz_tickets;
  string("\001") rpos_geschaftsform_kenn;
  string("\001") vas_kenn;
  decimal("\001") verkauftes_basisprodukt_id;
  string("\001") rahmenvertrag_id = '"'"'#'"'"';
  decimal("\001") dwh_vertrag_id = 0;
  decimal("\001") dwh_gp_id = 0;
  decimal("\001") dwh_konto_id = 0;
  decimal("\001") dwh_tarifgr_id = 0;
  string("\001") vo_kenn = '"'"'#'"'"';
  string("\001") zv_id = '"'"'0'"'"';
  datetime("YYYYMMDDHH24MISS")("\001") gueltig_von = NULL;
  datetime("YYYYMMDDHH24MISS")("\001") gueltig_bis = NULL;
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Join_with_dwh_ta_c_vertrag_1-26.dml'
print -r -- 'out::reformat(in) =
begin
  out.* :: in.*;
end;' > "${_AB_PROXY_DIR}"'/Filter_out_where_rpos_geschaeftsform_kenn_S_-27.xfr'
print -r -- 'record
/* 
*/
  date("YYYYMM")("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp;
  string("\001") prob_vertrag_id;
  string("\001") prob_provider_kenn;
  decimal("\001") anz_leistungen;
  decimal("\001") anz_tickets;
  string("\001") rpos_geschaftsform_kenn;
  string("\001") vas_kenn;
  string("\001", maximum_length=10) rahmenvertrag_id; /* VARCHAR2(10)*/
  decimal("\001", maximum_length=19) dwh_vertrag_id = NULL; /* NUMBER(16) NOT NULL*/
  decimal("\001", maximum_length=19) dwh_gp_id; /* NUMBER(16)*/
  decimal("\001", maximum_length=19) dwh_konto_id; /* NUMBER(16)*/
  decimal("\001", maximum_length=102) dwh_tarifgr_id; /* NUMBER*/
  string("\001", maximum_length=5) vo_kenn; /* CHAR(5)*/
  datetime("YYYYMMDDHH24MISS")("\001") gueltig_von = NULL; /* DATE NOT NULL*/
  datetime("YYYYMMDDHH24MISS")("\001") gueltig_bis = NULL; /* DATE*/
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Filter_out_where_rpos_geschaeftsform_kenn_S_-28.dml'
print -r -- 'out::reformat(in) =
begin
  let date("YYYYMMDD") month_last_day =(date('"'"'YYYYMMDD'"'"')) datetime_add(in.monats_id,date_month_end(date_month(in.monats_id),date_year(in.monats_id)));
  let integer(4) valid_flag =if ((is_null(in.gueltig_von) or month_last_day > in.gueltig_von)
and (is_null(in.gueltig_bis) or month_last_day <= in.gueltig_bis))
0
else
1;

  out.* :: in.*;
  out.rahmenvertrag_id :: if(valid_flag == 0)
in.rahmenvertrag_id;
  out.dwh_vertrag_id :: if(valid_flag == 0)
in.dwh_vertrag_id;
  out.dwh_gp_id :: if(valid_flag == 0)
in.dwh_gp_id;
  out.dwh_konto_id :: if(valid_flag == 0)
in.dwh_konto_id;
  out.dwh_tarifgr_id :: if(valid_flag == 0)
in.dwh_tarifgr_id;  /*NUMBER*/
  out.vo_kenn :: if(valid_flag == 0)
in.vo_kenn;
  out.gueltig_von :: if(valid_flag == 0)
in.gueltig_von;
end;' > "${_AB_PROXY_DIR}"'/Proof_Join_criteriase_gueltig_von_and_gueltig_bis-29.xfr'
print -r -- 'record
/* 
*/
  date("YYYYMM")("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp;
  string("\001") prob_vertrag_id;
  string("\001") prob_provider_kenn;
  decimal("\001") anz_leistungen;
  decimal("\001") anz_tickets;
  string("\001") rpos_geschaftsform_kenn;
  string("\001") vas_kenn;
  string("\001", maximum_length=10) rahmenvertrag_id = '"'"'#'"'"'; /* VARCHAR2(10)*/
  decimal("\001", maximum_length=19) dwh_vertrag_id = NULL; /* NUMBER(16) NOT NULL*/
  decimal("\001", maximum_length=19) dwh_gp_id = 0; /* NUMBER(16)*/
  decimal("\001", maximum_length=19) dwh_konto_id = 0; /* NUMBER(16)*/
  decimal("\001", maximum_length=102) dwh_tarifgr_id = 0; /* NUMBER*/
  string("\001", maximum_length=5) vo_kenn = '"'"'#'"'"'; /* CHAR(5)*/
  datetime("YYYYMMDDHH24MISS")("\001") gueltig_von = NULL; /* DATE NOT NULL*/
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Proof_Join_criteriase_gueltig_von_and_gueltig_bis-30.dml'
print -r -- 'type temporary_type = 
record
  integer(1) first_time;
  integer(1) valid_flag;
end /* Temporary variable*/;


/*This function may be optionally defined. 
Initialize temporary*/
temp::initialize(in) =
begin
  temp.first_time :: 1;
  temp.valid_flag :: 1;
end;



/*Do computation*/
temp::scan(temp, in) =
begin
  let integer(1) my_valid_flag = 1;

if(temp.first_time == 1)
    my_valid_flag = 0; 
  else 
  begin
     if (temp.first_time == 0 && !is_null(in.gueltig_von))
     my_valid_flag = 0;
     else
     my_valid_flag = 1;
  end

  temp.first_time :: 0;
  temp.valid_flag :: my_valid_flag;
end;



/*Create output record*/
out::finalize(temp, in) =
begin
  out.valid_flag :: temp.valid_flag;
  out.* :: in.*;
end;' > "${_AB_PROXY_DIR}"'/Scan_Mark_valid_historized_datasets-31.xfr'
print -r -- 'record
/* 
*/
  date("YYYYMM")("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp;
  string("\001") prob_vertrag_id;
  string("\001") prob_provider_kenn;
  decimal("\001") anz_leistungen;
  decimal("\001") anz_tickets;
  string("\001") rpos_geschaftsform_kenn;
  string("\001") vas_kenn;
  string("\001", maximum_length=10) rahmenvertrag_id = '"'"'#'"'"'; /* VARCHAR2(10)*/
  decimal("\001", maximum_length=19) dwh_vertrag_id = NULL; /* NUMBER(16) NOT NULL*/
  decimal("\001", maximum_length=19) dwh_gp_id = 0; /* NUMBER(16)*/
  decimal("\001", maximum_length=19) dwh_konto_id = 0; /* NUMBER(16)*/
  decimal("\001", maximum_length=102) dwh_tarifgr_id = 0; /* NUMBER*/
  string("\001", maximum_length=5) vo_kenn = '"'"'#'"'"'; /* CHAR(5)*/
  datetime("YYYYMMDDHH24MISS")("\001") gueltig_von = NULL; /* DATE NOT NULL*/
  integer(1) valid_flag;
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Scan_Mark_valid_historized_datasets-32.dml'
print -r -- '/*Im Kontext dwh_vertrag_id und gueltig_von muss
hier f�r die korrekte Durchf�hrung der Rankings 
im Fall von NULL der Wert \000 (NUL) gesetzt werden.*/
out::reformat(in) =
begin
  let string("\001") tmp_dwh_vertrag_id =string_concat('"'"'000000000000000'"'"',in.dwh_vertrag_id);

  out.* :: in.*;
  out.dwh_vertrag_id :: if(is_null(in.dwh_vertrag_id))
'"'"'\000'"'"'
else
string_substring(tmp_dwh_vertrag_id,string_length(tmp_dwh_vertrag_id)-15,16);
  out.gueltig_von :: if(is_null(in.gueltig_von))
'"'"'\000'"'"'
else
in.gueltig_von;
end;' > "${_AB_PROXY_DIR}"'/Filter_out_invalid_data-33.xfr'
print -r -- 'record
/* 
*/
  date("YYYYMM")("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp;
  string("\001") prob_vertrag_id;
  string("\001") prob_provider_kenn;
  decimal("\001") anz_leistungen;
  decimal("\001") anz_tickets;
  string("\001") rpos_geschaftsform_kenn;
  string("\001") vas_kenn;
  string("\001", maximum_length=10) rahmenvertrag_id = '"'"'#'"'"'; /* VARCHAR2(10)*/
  string("\001", maximum_length=19) dwh_vertrag_id = NULL; /* NUMBER(16) NOT NULL*/
  decimal("\001", maximum_length=19) dwh_gp_id = 0; /* NUMBER(16)*/
  decimal("\001", maximum_length=19) dwh_konto_id = 0; /* NUMBER(16)*/
  decimal("\001", maximum_length=102) dwh_tarifgr_id = 0; /* NUMBER*/
  string("\001", maximum_length=5) vo_kenn = '"'"'#'"'"'; /* CHAR(5)*/
  string("\001") gueltig_von = NULL; /* DATE NOT NULL*/
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Filter_out_invalid_data-34.dml'
print -r -- 'type temporary_type = 
record
  integer(1) first_time;
  decimal('"'"'|'"'"') rank;
  decimal('"'"'|'"'"') rank_increase;
  string('"'"'|'"'"') last_gueltig_von = NULL;
  string('"'"'|'"'"') last_dwh_vertrag_id = NULL;
end /* Temporary variable*/;


/*This function may be optionally defined. 
Initialize temporary*/
temp::initialize(in) =
begin
  temp.first_time :: 1;
  temp.rank :: 0;
  temp.rank_increase :: 1;
  temp.last_gueltig_von :: "";
  temp.last_dwh_vertrag_id :: "";
end;



/*Do computation*/
temp::scan(temp, in) =
begin
  let decimal('"'"'|'"'"') rank = 0;
  let decimal('"'"'|'"'"') rank_increase = 0;

if (! temp.first_time && in.dwh_vertrag_id == temp.last_dwh_vertrag_id && 
                         in.gueltig_von == temp.last_gueltig_von)                          
  begin
     rank = temp.rank;
     rank_increase = temp.rank_increase + 1;
  end
  else
  begin
     rank = temp.rank + temp.rank_increase;
     rank_increase = 1;
  end

  temp.first_time :: 0;
  temp.rank :: rank;
  temp.rank_increase :: rank_increase;
  temp.last_gueltig_von :: in.gueltig_von;
  temp.last_dwh_vertrag_id :: in.dwh_vertrag_id;
end;



/*Create output record*/
out::finalize(temp, in) =
begin
  out.* :: in.*;
  out.rankindex :: temp.rank;
  out.monats_id :: (decimal("\001"))(string("\001"))in.monats_id;
  out.dwh_vertrag_id :: if(in.dwh_vertrag_id!='"'"'\000'"'"')in.dwh_vertrag_id;
end;' > "${_AB_PROXY_DIR}"'/Scan_Ranking_over_gueltig_von_dwh_vertrag_id_desc-35.xfr'
print -r -- 'record
  decimal("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp;
  string("\001") prob_vertrag_id;
  string("\001") prob_provider_kenn;
  decimal("\001") anz_leistungen;
  decimal("\001") anz_tickets;
  string("\001") rpos_geschaftsform_kenn;
  string("\001") vas_kenn;
  string("\001", maximum_length=10) rahmenvertrag_id; /* VARCHAR2(10)*/
  decimal("\001", maximum_length=19) dwh_vertrag_id = 0; /* NUMBER(16) NOT NULL*/
  decimal("\001", maximum_length=19) dwh_gp_id; /* NUMBER(16)*/
  decimal("\001", maximum_length=19) dwh_konto_id; /* NUMBER(16)*/
  decimal("\001", maximum_length=102) dwh_tarifgr_id; /* NUMBER*/
  string("\001", maximum_length=5) vo_kenn; /* CHAR(5)*/
  decimal("\001") rankindex = 0;
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Scan_Ranking_over_gueltig_von_dwh_vertrag_id_desc-36.dml'
print -r -- 'out::rollup(in) =
begin
  out.* :: in.*;
  out.rechpos_brutto_eur :: sum(in.rechpos_brutto_eur);
  out.rechpos_netto_eur :: sum(in.rechpos_netto_eur);
  out.rechpos_mwst_eur :: sum(in.rechpos_mwst_eur);
  out.anz_leistungen :: sum(in.anz_leistungen);
  out.anz_tickets :: sum(in.anz_tickets);
end;' > "${_AB_PROXY_DIR}"'/Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_anz_leistungen_anz_tickets-37.xfr'
print -r -- '/*Das Ladedatum wird auf Seiten von Informatica beim
select via SYSDATE gesetzt. 
Da dies nur ein Datum zur�ckgibt, 
wird hier now1() verwendet.*/
out::reformat(in) =
begin
  out.* :: in.*;
  out.rpos_geschaftsform_kenn :: if(in.rpos_geschaftsform_kenn=='"'"'F'"'"')
   if(in.vas_kenn == '"'"'P30002'"'"')
   '"'"'G'"'"'
   else
   in.rpos_geschaftsform_kenn
else
in.rpos_geschaftsform_kenn;
  out.ladedatum :: now1();
end;' > "${_AB_PROXY_DIR}"'/Decode_rpos_geschaeftsform_kenn-38.xfr'
print -r -- 'record
  decimal("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp;
  string("\001") prob_vertrag_id;
  string("\001") prob_provider_kenn;
  decimal("\001") anz_leistungen;
  decimal("\001") anz_tickets;
  string("\001") rpos_geschaftsform_kenn;
  string("\001") vas_kenn;
  string("\001", maximum_length=10) rahmenvertrag_id; /* VARCHAR2(10)*/
  decimal("\001", maximum_length=19) dwh_vertrag_id = 0; /* NUMBER(16) NOT NULL*/
  decimal("\001", maximum_length=19) dwh_gp_id; /* NUMBER(16)*/
  decimal("\001", maximum_length=19) dwh_konto_id; /* NUMBER(16)*/
  decimal("\001", maximum_length=102) dwh_tarifgr_id; /* NUMBER*/
  string("\001", maximum_length=5) vo_kenn; /* CHAR(5)*/
  datetime("YYYYMMDDHH24MISS")("\001") ladedatum;
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Decode_rpos_geschaeftsform_kenn-39.dml'
print -r -- 'out::reformat(in) =
begin
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
  out.rahmenvertrag :: in.rahmenvertrag_id;
end;' > "${_AB_PROXY_DIR}"'/Reformat_for_insert_Factoring_Rechnungen_-40.xfr'
print -r -- 'out::reformat(in) =
begin
  out.* :: in.*;
  out.rech_leistung_id_carm :1: string_substring(in.rech_leistung_id_carm,1,9);
  out.rahmenvertrag :: in.rahmenvertrag_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
end;' > "${_AB_PROXY_DIR}"'/Reformat_for_insert_Factoring_Gutschriften_-42.xfr'
print -r -- 'out::reformat(in) =
begin
  let date("YYYYMMDD") month_last_day =(date('"'"'YYYYMMDD'"'"'))datetime_add(in.monats_id,date_month_end(date_month(in.monats_id),date_year(in.monats_id)));
  let integer(4) valid_flag =if ((is_null(in.gueltig_von) or month_last_day > in.gueltig_von) 
and (is_null(in.gueltig_bis) or month_last_day <= in.gueltig_bis))
0
else
1;

  out.* :: in.*;
  out.rahmenvertrag_id :: if(valid_flag == 0)
in.rahmenvertrag_id;
  out.dwh_vertrag_id :: if(valid_flag == 0)
in.dwh_vertrag_id;
  out.dwh_gp_id :: if(valid_flag == 0)
in.dwh_gp_id;
  out.dwh_konto_id :: if(valid_flag == 0)
in.dwh_konto_id;
  out.dwh_tarifgr_id :: if(valid_flag == 0)
in.dwh_tarifgr_id;  /*NUMBER*/
  out.vo_kenn :: if(valid_flag == 0)
in.vo_kenn;
  out.zv_id :: if(valid_flag == 0)
in.zv_id;
  out.gueltig_von :: if(valid_flag == 0)
in.gueltig_von;
end;' > "${_AB_PROXY_DIR}"'/Proof_Join_criterias_gueltig_von_and_gueltig_bis-45.xfr'
print -r -- 'record
  date("YYYYMM")("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp;
  string("\001") pooling;
  decimal("\001") rechnungvertrag_id;
  decimal("\001") verkauftes_basisprodukt_id = NULL("");
  string("\001") rahmenvertrag_id = '"'"'#'"'"';
  decimal("\001") dwh_vertrag_id = NULL;
  decimal("\001") dwh_gp_id = 0;
  decimal("\001") dwh_konto_id = 0;
  decimal("\001") dwh_tarifgr_id = 0;
  string("\001") vo_kenn = '"'"'#'"'"';
  string("\001") zv_id = '"'"'0'"'"';
  datetime("YYYYMMDDHH24MISS")("\001") gueltig_von = NULL;
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Proof_Join_criterias_gueltig_von_and_gueltig_bis-46.dml'
print -r -- 'record
/* 
*/
  date("YYYYMM")("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp;
  string("\001") pooling;
  decimal("\001") rechnungvertrag_id;
  decimal("\001") verkauftes_basisprodukt_id = NULL("");
  string("\001", maximum_length=10) rahmenvertrag_id = '"'"'#'"'"'; /* VARCHAR2(10)*/
  decimal("\001", maximum_length=19) dwh_vertrag_id = NULL; /* NUMBER(16) NOT NULL*/
  decimal("\001", maximum_length=19) dwh_gp_id = 0; /* NUMBER(16)*/
  decimal("\001", maximum_length=19) dwh_konto_id = 0; /* NUMBER(16)*/
  decimal("\001", maximum_length=102) dwh_tarifgr_id = 0; /* NUMBER*/
  string("\001", maximum_length=5) vo_kenn = '"'"'#'"'"'; /* CHAR(5)*/
  string("\001", maximum_length=10) zv_id = '"'"'0'"'"'; /* VARCHAR2(10)*/
  datetime("YYYYMMDDHH24MISS")("\001") gueltig_von = NULL; /* DATE NOT NULL*/
  integer(1) valid_flag;
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Scan_Mark_valid_historized_datasets-47.dml'
print -r -- 'record
/* 
*/
  date("YYYYMM")("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp;
  string("\001") pooling;
  decimal("\001") rechnungvertrag_id;
  decimal("\001") verkauftes_basisprodukt_id = NULL("");
  string("\001", maximum_length=10) rahmenvertrag_id = '"'"'#'"'"'; /* VARCHAR2(10)*/
  string("\001", maximum_length=19) dwh_vertrag_id = NULL; /* NUMBER(16) NOT NULL*/
  decimal("\001", maximum_length=19) dwh_gp_id = 0; /* NUMBER(16)*/
  decimal("\001", maximum_length=19) dwh_konto_id = 0; /* NUMBER(16)*/
  decimal("\001", maximum_length=102) dwh_tarifgr_id = 0; /* NUMBER*/
  string("\001", maximum_length=5) vo_kenn = '"'"'#'"'"'; /* CHAR(5)*/
  string("\001", maximum_length=10) zv_id = '"'"'0'"'"'; /* VARCHAR2(10)*/
  string("\001") gueltig_von = NULL; /* DATE NOT NULL*/
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Filter_out_invalid_data-48.dml'
print -r -- 'type temporary_type = 
record
  integer(1) first_time;
  decimal('"'"'|'"'"') rank;
  decimal('"'"'|'"'"') rank_increase;
  string('"'"'|'"'"') last_gueltig_von = NULL;
  string('"'"'|'"'"') last_dwh_vertrag_id = NULL;
end /* Temporary variable*/;


/*This function may be optionally defined. 
Initialize temporary*/
temp::initialize(in) =
begin
  temp.first_time :: 1;
  temp.rank :: 0;
  temp.rank_increase :: 1;
  temp.last_gueltig_von :: "";
  temp.last_dwh_vertrag_id :: "";
end;



/*Do computation*/
temp::scan(temp, in) =
begin
  let decimal('"'"'|'"'"') rank = 0;
  let decimal('"'"'|'"'"') rank_increase = 0;

if (! temp.first_time && in.gueltig_von == temp.last_gueltig_von && 
                         in.dwh_vertrag_id == temp.last_dwh_vertrag_id )                          
  begin
     rank = temp.rank;
     rank_increase = temp.rank_increase + 1;
  end
  else
  begin
     rank = temp.rank + temp.rank_increase;
     rank_increase = 1;
  end

  temp.first_time :: 0;
  temp.rank :: rank;
  temp.rank_increase :: rank_increase;
  temp.last_gueltig_von :: in.gueltig_von;
  temp.last_dwh_vertrag_id :: in.dwh_vertrag_id;
end;



/*Create output record*/
out::finalize(temp, in) =
begin
  out.* :: in.*;
  out.rankindex :: temp.rank;
  out.monats_id :: (decimal("\001"))(string("\001"))in.monats_id;
  out.dwh_vertrag_id :: if(in.dwh_vertrag_id!='"'"'\000'"'"')
in.dwh_vertrag_id;
end;' > "${_AB_PROXY_DIR}"'/Scan_Ranking_over_gueltig_von_desc_dwh_vertrag_id_desc-49.xfr'
print -r -- 'record
/* Obwohl monats_id sp�ter als decimal
in die Datenbank geschrieben wird, findet
hier eine Konvertierung in ein Date statt,
da f�r eine sp�tere Pr�fung ein
korrekter Datumswert gesetzt sein muss.
*/
  decimal("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp;
  string("\001") pooling;
  decimal("\001") rechnungvertrag_id;
  decimal("\001") verkauftes_basisprodukt_id;
  string("\001", maximum_length=10) rahmenvertrag_id; /* VARCHAR2(10)*/
  decimal("\001", maximum_length=19) dwh_vertrag_id = 0; /* NUMBER(16) NOT NULL*/
  decimal("\001", maximum_length=19) dwh_gp_id; /* NUMBER(16)*/
  decimal("\001", maximum_length=19) dwh_konto_id; /* NUMBER(16)*/
  decimal("\001", maximum_length=102) dwh_tarifgr_id; /* NUMBER*/
  string("\001", maximum_length=5) vo_kenn; /* CHAR(5)*/
  string("\001", maximum_length=10) zv_id; /* VARCHAR2(10)*/
  decimal("\001") rankindex = 0;
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Scan_Ranking_over_gueltig_von_desc_dwh_vertrag_id_desc-50.dml'
print -r -- '/*Das Ladedatum wird auf Seiten von Informatica beim
select via SYSDATE gesetzt. 
Da dies nur ein Datum zur�ckgibt, 
wird hier now1() verwendet.*/
out::rollup(in) =
begin
  out.* :: in.*;
  out.rechpos_brutto_eur :: sum(in.rechpos_brutto_eur);
  out.rechpos_netto_eur :: sum(in.rechpos_netto_eur);
  out.rechpos_mwst_eur :: sum(in.rechpos_mwst_eur);
  out.ladedatum :: now1();
  out.typ :: if(((in.rech_leistung_id_carm == '"'"'RABATT'"'"' && in.vertrags_id == 0) || in.pooling == '"'"'P'"'"'))
'"'"'T'"'"';
end;' > "${_AB_PROXY_DIR}"'/Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur-51.xfr'
print -r -- 'record
/* Obwohl monats_id sp�ter als decimal
in die Datenbank geschrieben wird, findet
hier eine Konvertierung in ein Date statt,
da f�r eine sp�tere Pr�fung ein
korrekter Datumswert gesetzt sein muss.
*/
  decimal("\001") monats_id;
  string("\001") debitor_id;
  string("\001") kontier_grp_id = '"'"'#'"'"';
  string("\001") rechnung_id;
  date("YYYYMMDD")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  decimal("\001") vertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") rechpos_brutto_eur;
  decimal("\001") rechpos_netto_eur;
  decimal("\001") rechpos_mwst_eur;
  string("\001") abs_grp;
  string("\001") pooling;
  decimal("\001") rechnungvertrag_id;
  decimal("\001") verkauftes_basisprodukt_id;
  string("\001", maximum_length=10) rahmenvertrag_id; /* VARCHAR2(10)*/
  decimal("\001", maximum_length=19) dwh_vertrag_id = 0; /* NUMBER(16) NOT NULL*/
  decimal("\001", maximum_length=19) dwh_gp_id; /* NUMBER(16)*/
  decimal("\001", maximum_length=19) dwh_konto_id; /* NUMBER(16)*/
  decimal("\001", maximum_length=102) dwh_tarifgr_id; /* NUMBER*/
  string("\001", maximum_length=5) vo_kenn; /* CHAR(5)*/
  string("\001", maximum_length=10) zv_id; /* VARCHAR2(10)*/
  datetime("YYYYMMDDHH24MISS")("\001") ladedatum;
/* T bedeutet tempor�rer Satz
F bedeutet Faktensatz (Default)*/
  string("\001") typ = '"'"'F'"'"';
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur-52.dml'
print -r -- 'out::reformat(in) =
begin
  out.* :: in.*;
  out.rahmenvertrag :: in.rahmenvertrag_id;
end;' > "${_AB_PROXY_DIR}"'/Reformat_for_insert_fact_data_-53.xfr'
print -r -- 'out::reformat(in) =
begin
  let datetime("YYYYMMDDHH24MISS") mindate =(datetime('"'"'YYYYMMDDHH24MISS'"'"'))(string(14))'"'"'19000101000000'"'"';

  out.* :: in.*;
  out.bearbeitung_datum :: mindate;
end;' > "${_AB_PROXY_DIR}"'/Reformat_for_insert_temporary_data_-55.xfr'
print -r -- 'record
  string("\001") debitor_id;
  string("\001") rechnung_id;
  datetime("YYYYMMDDHH24MISS")("\001") rechnung_datum;
  decimal("\001") standardvertrags_id = 0;
  string("\001") rech_leistung_id_carm;
  decimal("\001") vertrags_id = 0;
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Reformat_rechnung_datum_to_datetime_for_Delete-57.dml'
print -r -- 'out::reformat(in) =
begin
  out.rechnung_id :: in.rechnung_id;
  out.rechnung_datum :: in.rechnung_datum;
  out.standardvertrags_id :: in.standardvertrags_id;
  out.vertrags_id :: in.vertrags_id;
  out.rech_leistung_id_carm :: in.rech_leistung_id_carm;
end;' > "${_AB_PROXY_DIR}"'/Reformat_for_delete-58.xfr'
print -r -- 'record
  string("\001", maximum_length=14) rechnung_id; /* VARCHAR2(14) NOT NULL*/
  date("YYYYMMDD")("\001") rechnung_datum; /* DATE NOT NULL*/
  decimal("\001") standardvertrags_id;
  decimal("\001") vertrags_id;
  string("\001", maximum_length=10) rech_leistung_id_carm; /* VARCHAR2(10) NOT NULL*/
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Reformat_for_delete-59.dml'
print -r -- 'DELETE FROM DWH$TA_F_GPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id' > "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_F_GPOS_FACT_CARM-60.sql'
print -r -- 'DELETE FROM DWH$TA_F_RPOS_CARM
WHERE  rechnung_datum = :rechnung_datum
AND    rechnung_id = :rechnung_id
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id' > "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_F_RPOS_CARM_2-61.sql'
print -r -- 'DELETE FROM DWH$TA_F_RPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id' > "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_F_RPOS_FACT_CARM-62.sql'
print -r -- 'DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id' > "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_F_RPOS_RESELLING_CARM-63.sql'
print -r -- 'record
  string("\001", maximum_length=14) rechnung_id; /* VARCHAR2(14) NOT NULL*/
  date("YYYYMMDD")("\001") rechnung_datum; /* DATE NOT NULL*/
  string("\001", maximum_length=13) debitor_id; /* VARCHAR2(13) NOT NULL*/
  string(1) newline = "\n";
end;' > "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_T_RPOS_CARM-64.dml'
print -r -- 'DELETE FROM DWH$TA_T_RPOS_CARM
WHERE  debitor_id = :debitor_id
AND    rechnung_datum = :rechnung_datum
AND    rechnung_id = :rechnung_id' > "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_T_RPOS_CARM-65.sql'
print -r -- 'record
  string(";") kennzeichen;
  string(";") bemerkung;
  string(";") stichtag;
  string(";") anzahl;
  string(";") inhalt;
  string("\n") erstellt_am;
end;' > "${_AB_PROXY_DIR}"'/Format_Enderecord-66.dml'
print -r -- 'out::reformat(in) =
begin
  out.kennzeichen :: in.kennzeichen;
  out.bemerkung :: in.bemerkung;
  out.stichtag :: in.stichtag;
  out.anzahl :: in.anzahl;
  out.inhalt :: in.inhalt;
  out.erstellt_am :: (string_index(in.erstellt_am, ";") == 0) ? in.erstellt_am : string_substring(in.erstellt_am, 1, string_length(in.erstellt_am)-1);
end;' > "${_AB_PROXY_DIR}"'/Reformat_Enderecord_for_Processing-67.xfr'
print -r -- 'out::reformat(in) =
begin
  out.monats_id :: (string(6))(date("YYYYMM"))date_add_months((date("YYYYMM")) string_substring(in.stichtag,1,6),-1);
  out.abs_grp :: string_substring(in.bemerkung,10,5) ;
  out.dateiname :: in.bemerkung;
  out.rechnung_datum :: (date("YYYYMMDD")) in.stichtag;
  out.rechnungsteil :: (string(1))"P";
  out.ladedatum :: now();
end;' > "${_AB_PROXY_DIR}"'/Reformat_for_DB_and_Filter_out_where_Kompl_Kennzeichen_L_-68.xfr'
print -r -- 'UPDATE DWH$TA_K_RECH_ABSGRP
SET   rechnung_datum = :rechnung_datum, 
      ladedatum = :ladedatum
WHERE  monats_id = :monats_id
AND    abs_grp = :abs_grp
AND    dateiname = :dateiname
AND    rechnungsteil = :rechnungsteil' > "${_AB_PROXY_DIR}"'/Update_Insert_DWH_TA_K_RECH_ABSGRP-70.sql'
print -r -- 'INSERT INTO DWH$TA_K_RECH_ABSGRP (monats_id, abs_grp, dateiname,  rechnung_datum, rechnungsteil, ladedatum)
VALUES (:monats_id, :abs_grp, :dateiname,  :rechnung_datum, :rechnungsteil, :ladedatum)' > "${_AB_PROXY_DIR}"'/Update_Insert_DWH_TA_K_RECH_ABSGRP-71.sql'
print -r -- '/*Reformat operation*/
out::reformat(in) =
begin
        out.dateiname  :: "'"${BHB_Dateiname}"'";
        out.eintragsnr :: '"${BHB_Eintragsnr}"';
        out.bemerkung  :: in.bemerkung;
        out.anzahl     :: in.anzahl;
        out.inhalt     :: in.inhalt;
end;' > "${_AB_PROXY_DIR}"'/Reformat_Enderecord_for_Update-72.xfr'
print -r -- 'record
  string(";") dateiname;
  decimal(";") eintragsnr;
  string(";") bemerkung;
  decimal(";") anzahl;
  string("\n") inhalt;
end;' > "${_AB_PROXY_DIR}"'/Reformat_Enderecord_for_Update-73.dml'
print -r -- 'update dwh$ta_k_meldungen 
set anzahl_ds_eof = :anzahl
  , dateiname = :dateiname
  , enderecord_text = :inhalt
  , zusatzinfo = :bemerkung 
where entrynr = :eintragsnr' > "${_AB_PROXY_DIR}"'/Update_DWH_TA_K_MELDUNGEN-74.sql'
print -r -- 'out::reformat(in) =
begin
  out.vertrags_id :: in.vertrags_id;
  out.monats_id :: in.monats_id;
end;' > "${_AB_PROXY_DIR}"'/Reformat_fur_testzwecke-75.xfr'

mp job ${AB_JOB}

# Layouts:
mp layout layout1 -hosts localhost localhost localhost localhost localhost localhost localhost localhost localhost localhost localhost localhost localhost localhost localhost localhost
mp layout layout2 .
m_db_layout layout3 ${BHB_DB}/DWH_BHB.dbc -serial
m_db_layout layout4 "${BHB_DB}"'/DWH_BHB.dbc'  -serial

# Record Formats (Metadata):
mp metadata metadata1 -file "${_AB_PROXY_DIR}"'/dwh_ta_c_vertrag-12.dml'
mp metadata metadata2 -file "${_AB_PROXY_DIR}"'/Read_File-13.dml'
mp metadata metadata3 -file "${_AB_PROXY_DIR}"'/Read_File-15.dml'
mp metadata metadata4 -file "${_AB_PROXY_DIR}"'/Reformat_Data-16.dml'
mp metadata metadata5 -file "${BHB_SAP_DML}"'/carmen_b_pos_dml.dml'
mp metadata metadata6 -file "${_AB_PROXY_DIR}"'/Reformat_for_DB-21.dml'
mp metadata metadata7 -file "${_AB_PROXY_DIR}"'/Validate_Records-23.dml'
mp metadata metadata8 -file "${_AB_PROXY_DIR}"'/Join_with_dwh_ta_c_vertrag_1-26.dml'
mp metadata metadata9 -file "${_AB_PROXY_DIR}"'/Filter_out_where_rpos_geschaeftsform_kenn_S_-28.dml'
mp metadata metadata10 -file "${_AB_PROXY_DIR}"'/Proof_Join_criteriase_gueltig_von_and_gueltig_bis-30.dml'
mp metadata metadata11 -file "${_AB_PROXY_DIR}"'/Scan_Mark_valid_historized_datasets-32.dml'
mp metadata metadata12 -file "${_AB_PROXY_DIR}"'/Filter_out_invalid_data-34.dml'
mp metadata metadata13 -file "${_AB_PROXY_DIR}"'/Scan_Ranking_over_gueltig_von_dwh_vertrag_id_desc-36.dml'
mp metadata metadata14 -file "${_AB_PROXY_DIR}"'/Decode_rpos_geschaeftsform_kenn-39.dml'
mp metadata metadata15 -file "${BHB_DML}"'/dwh_ta_f_rpos_fact_carm.dml'
mp metadata metadata16 -file "${BHB_DML}"'/dwh_ta_f_gpos_fact_carm.dml'
mp metadata metadata17 -file "${BHB_DML}"'/dwh_ta_f_rpos_reselling_carm.dml'
mp metadata metadata18 -file "${_AB_PROXY_DIR}"'/Proof_Join_criterias_gueltig_von_and_gueltig_bis-46.dml'
mp metadata metadata19 -file "${_AB_PROXY_DIR}"'/Scan_Mark_valid_historized_datasets-47.dml'
mp metadata metadata20 -file "${_AB_PROXY_DIR}"'/Filter_out_invalid_data-48.dml'
mp metadata metadata21 -file "${_AB_PROXY_DIR}"'/Scan_Ranking_over_gueltig_von_desc_dwh_vertrag_id_desc-50.dml'
mp metadata metadata22 -file "${_AB_PROXY_DIR}"'/Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur-52.dml'
mp metadata metadata23 -file "${BHB_DML}"'/dwh_ta_f_rpos_carm.dml'
mp metadata metadata24 -file "${BHB_DML}"'/dwh_ta_t_rpos_carm.dml'
mp metadata metadata25 -file "${_AB_PROXY_DIR}"'/Reformat_rechnung_datum_to_datetime_for_Delete-57.dml'
mp metadata metadata26 -file "${_AB_PROXY_DIR}"'/Reformat_for_delete-59.dml'
mp metadata metadata27 -file "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_T_RPOS_CARM-64.dml'
mp metadata metadata28 -file "${_AB_PROXY_DIR}"'/Format_Enderecord-66.dml'
mp metadata metadata29 -file "${BHB_DML}"'/dwh_ta_k_rech_absgrp.dml'
mp metadata metadata30 -file "${_AB_PROXY_DIR}"'/Reformat_Enderecord_for_Update-73.dml'

export AB_CATALOG;AB_CATALOG=${AB_CATALOG:-"${XX_CATALOG}"}
# Catalog Usage: Creating temporary catalog using lookup files only
m_rmcatalog -catalog GDE-map_rpos_carmen_import-${AB_JOB}.cat > /dev/null 2>&1
m_mkcatalog -catalog GDE-map_rpos_carmen_import-${AB_JOB}.cat
SAVED_CATALOG="${AB_CATALOG}"
export AB_CATALOG;AB_CATALOG='GDE-map_rpos_carmen_import-'"${AB_JOB}"'.cat'

# Components in phase 0:
# mp itable Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.DWH_TA_F_GPOS_FACT_CARM_2__table_ "${BHB_DB}"'/DWH_BHB.dbc' -table 'DWH$TA_F_GPOS_FACT_CARM' -interface api -field_type_preference delimited -layout layout4
# mp local-sort Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id}' -max-core 100663296 -layout ???
# mp dedup Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp sort-groups Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Sort_within_Groups_Sort_over_rech_leistung_id_carm -major-key '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id}' -minor-key '{rech_leistung_id_carm}' -max-core 10485760 -layout ???
# mp itable Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.DWH_TA_F_GPOS_FACT_CARM__table_ "${BHB_DB}"'/DWH_BHB.dbc' -table 'DWH$TA_F_GPOS_FACT_CARM' -interface api -field_type_preference delimited -layout layout4
# mp local-sort Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' -max-core 100663296 -layout ???
# mp dedup Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp merge-join Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Determine_rows_to_be_deleted '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' "${_AB_PROXY_DIR}"'/Determine_rows_to_be_deleted-2.xfr' -max-core 8388608 -limit 0 -ramp 0.0 -layout ???
# mp add-port Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Determine_rows_to_be_deleted.in.in0
# mp add-port Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Determine_rows_to_be_deleted.in.in1
# mp dedup Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Dedup_Sorted_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp itable Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.DWH_TA_F_RPOS_CARM_2__table_ "${BHB_DB}"'/DWH_BHB.dbc' -select 'select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_CARM' -interface api -field_type_preference delimited -layout layout4
# mp local-sort Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id}' -max-core 100663296 -layout ???
# mp dedup Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp sort-groups Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Sort_within_Groups_Sort_over_rech_leistung_id_carm -major-key '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id}' -minor-key '{rech_leistung_id_carm}' -max-core 10485760 -layout ???
# mp itable Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.DWH_TA_F_RPOS_CARM__table_ "${BHB_DB}"'/DWH_BHB.dbc' -select 'select rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_CARM' -interface api -field_type_preference delimited -layout layout4
# mp local-sort Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' -max-core 100663296 -layout ???
# mp merge-join Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Determine_rows_to_be_deleted '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' "${_AB_PROXY_DIR}"'/Determine_rows_to_be_deleted-3.xfr' -max-core 8388608 -limit 0 -ramp 0.0 -layout ???
# mp add-port Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Determine_rows_to_be_deleted.in.in0 -singlematch
# mp add-port Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Determine_rows_to_be_deleted.in.in1
# mp dedup Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Dedup_Sorted_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp db-update Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Delete_rows_from_DWH_TA_F_RPOS_CARM "${BHB_DB}"'/DWH_BHB.dbc' "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_F_RPOS_CARM-4.sql' ~null -interface api -new_db_update -layout ???
# mp db-lookup Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Join_with_DB "${BHB_DB}"'/DWH_BHB.dbc' 'select rechnung_id
# from DWH$TA_F_RPOS_CARM
# where rechnung_id = :rechnung_id
# and rechnung_datum = :rechnung_datum
# and standardvertrags_id = :standardvertrags_id
# and vertrags_id = :vertrags_id 
# and rech_leistung_id_carm = :rech_leistung_id_carm' "${_AB_PROXY_DIR}"'/Join_with_DB-6.xfr' ~null -match_required -maximum_matches -1 -commit_number -1 -limit 0 -ramp 0.0 -fixed_size_dml -generate_dml_with_nulls -select -layout ???
# mp select-transform Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Filter_by_Expression 'delete_flag == 1' -limit 0 -ramp 0.0 -layout ???
# mp dedup Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Dedup_Sorted_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_1 '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp db-update Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Delete_rows_from_DWH_TA_F_RPOS_CARM_1 "${BHB_DB}"'/DWH_BHB.dbc' "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_F_RPOS_CARM-4.sql' ~null -interface api -new_db_update -layout layout3
# mp itable Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.DWH_TA_F_RPOS_FACT_CARM_2__table_ "${BHB_DB}"'/DWH_BHB.dbc' -select 'select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_FACT_CARM' -interface api -field_type_preference delimited -layout layout4
# mp local-sort Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id}' -max-core 100663296 -layout ???
# mp dedup Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp sort-groups Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Sort_within_Groups_Sort_over_rech_leistung_id_carm -major-key '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id}' -minor-key '{rech_leistung_id_carm}' -max-core 10485760 -layout ???
# mp itable Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.DWH_TA_F_RPOS_FACT_CARM__table_ "${BHB_DB}"'/DWH_BHB.dbc' -select 'select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_FACT_CARM' -interface api -field_type_preference delimited -layout layout4
# mp local-sort Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' -max-core 100663296 -layout ???
# mp merge-join Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Determine_rows_to_be_deleted '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' "${_AB_PROXY_DIR}"'/Determine_rows_to_be_deleted-2.xfr' -max-core 8388608 -limit 0 -ramp 0.0 -layout ???
# mp add-port Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Determine_rows_to_be_deleted.in.in0 -singlematch
# mp add-port Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Determine_rows_to_be_deleted.in.in1
# mp db-lookup Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Join_with_DB_Determine_rows_to_be_deleted "${BHB_DB}"'/DWH_BHB.dbc' 'select
# rechnung_id,
# rechnung_datum,
# standardvertrags_id,
# vertrags_id,
# rech_leistung_id_carm
# from
# DWH$TA_F_RPOS_FACT_CARM
# where
# rechnung_id = :rechnung_id and
# rechnung_datum = :rechnung_datum and
# standardvertrags_id = :standardvertrags_id and
# vertrags_id = :vertrags_id and
# rech_leistung_id_carm = :rech_leistung_id_carm' "${_AB_PROXY_DIR}"'/Join_with_DB_Determine_rows_to_be_deleted-7.xfr' ~null -match_required -maximum_matches -1 -commit_number -1 -limit 0 -ramp 0.0 -fixed_size_dml -generate_dml_with_nulls -select -layout ???
# mp local-sort Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Sort_over_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id}' -max-core 100663296 -layout ???
# mp dedup Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Dedup_Sorted_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp itable Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.DWH_TA_F_RPOS_RESELLING_CARM_1__table_ "${BHB_DB}"'/DWH_BHB.dbc' -select 'select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm, debitor_id from DWH$TA_F_RPOS_RESELLING_CARM' -interface api -field_type_preference delimited -layout layout4
# mp local-sort Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id}' -max-core 100663296 -layout ???
# mp dedup Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; debitor_id}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp sort-groups Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Sort_within_Groups_Sort_over_rech_leistung_id_carm -major-key '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id}' -minor-key '{rech_leistung_id_carm}' -max-core 10485760 -layout ???
# mp itable Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.DWH_TA_F_RPOS_RESELLING_CARM__table_ "${BHB_DB}"'/DWH_BHB.dbc' -select 'select rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm from DWH$TA_F_RPOS_RESELLING_CARM' -interface api -field_type_preference delimited -layout layout4
# mp local-sort Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' -max-core 100663296 -layout ???
# mp merge-join Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Determine_rows_to_be_deleted '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' "${_AB_PROXY_DIR}"'/Determine_rows_to_be_deleted-2.xfr' -max-core 8388608 -limit 0 -ramp 0.0 -layout ???
# mp add-port Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Determine_rows_to_be_deleted.in.in0 -singlematch
# mp add-port Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Determine_rows_to_be_deleted.in.in1
# mp db-lookup Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Join_with_DB_Determine_rows_to_be_deleted "${BHB_DB}"'/DWH_BHB.dbc' 'select
# rechnung_id,
# rechnung_datum,
# standardvertrags_id,
# vertrags_id,
# rech_leistung_id_carm
# from
# DWH$TA_F_RPOS_RESELLING_CARM
# where
# rechnung_id = :rechnung_id and
# rechnung_datum = :rechnung_datum and
# standardvertrags_id = :standardvertrags_id and
# vertrags_id = :vertrags_id and
# rech_leistung_id_carm = :rech_leistung_id_carm' "${_AB_PROXY_DIR}"'/Join_with_DB_Determine_rows_to_be_deleted-7.xfr' ~null -match_required -maximum_matches -1 -commit_number -1 -limit 0 -ramp 0.0 -fixed_size_dml -generate_dml_with_nulls -select -layout ???
# mp local-sort Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Sort_over_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id}' -max-core 100663296 -layout ???
# mp dedup Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Dedup_Sorted_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp itable Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.DWH_TA_T_RPOS_CARM_2__table_ "${BHB_DB}"'/DWH_BHB.dbc' -table 'DWH$TA_T_RPOS_CARM' -interface api -field_type_preference delimited -layout layout4
# mp local-sort Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id}' -max-core 100663296 -layout ???
# mp dedup Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp merge-join Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Determine_rows_to_be_deleted '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id}' "${_AB_PROXY_DIR}"'/Determine_rows_to_be_deleted-8.xfr' -max-core 8388608 -limit 0 -ramp 0.0 -layout ???
# mp add-port Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Determine_rows_to_be_deleted.in.in0
# mp add-port Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Determine_rows_to_be_deleted.in.in1 -singlematch
# mp local-sort Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_rechnung_id_rechnung_datum_debitor_id '{rechnung_id; rechnung_datum; debitor_id}' -max-core 100663296 -layout ???
# mp dedup Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Dedup_Sorted_over_rechnung_id_rechnung_datum_debitor_id '{rechnung_id; rechnung_datum; debitor_id}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp itable Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.DWH_TA_T_RPOS_CARM__table_ "${BHB_DB}"'/DWH_BHB.dbc' -table 'DWH$TA_T_RPOS_CARM' -interface api -field_type_preference delimited -layout layout4
# mp local-sort Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_rechnung_id_rechnung_datum_debitor_id_1 '{rechnung_id; rechnung_datum; debitor_id}' -max-core 100663296 -layout ???
# mp dedup Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Dedup_Sorted_over_rechnung_id_rechnung_datum_debitor_id_1 '{rechnung_id; rechnung_datum; debitor_id}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp db-lookup Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Join_with_DB_Determine_rows_to_be_deleted "${BHB_DB}"'/DWH_BHB.dbc' 'select
# rechnung_id,
# rechnung_datum,
# debitor_id
# from
# DWH$TA_T_RPOS_CARM
# where
# rechnung_id = :rechnung_id and
# rechnung_datum = :rechnung_datum and
# debitor_id = :debitor_id' "${_AB_PROXY_DIR}"'/Join_with_DB_Determine_rows_to_be_deleted-9.xfr' ~null -match_required -maximum_matches -1 -commit_number -1 -limit 0 -ramp 0.0 -fixed_size_dml -generate_dml_with_nulls -select -layout ???
# mp local-sort Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_rechnung_id_rechnung_datum_debitor_id_3 '{rechnung_id; rechnung_datum; debitor_id}' -max-core 100663296 -layout ???
# mp dedup Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Dedup_Sorted_over_rechnung_id_rechnung_datum_debitor_id_2 '{rechnung_id; rechnung_datum; debitor_id}' -keep first -limit 0 -ramp 0.0 -check-sort -layout ???
# mp local-sort Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_rechnung_id_rechnung_datum_debitor_id_2 '{rechnung_id; rechnung_datum; debitor_id}' -max-core 100663296 -layout ???
# mp merge-join Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Determine_rows_to_be_deleted_incl_dedup_of_port_1_ '{rechnung_id; rechnung_datum; debitor_id}' "${_AB_PROXY_DIR}"'/Determine_rows_to_be_deleted-8.xfr' -max-core 8388608 -limit 0 -ramp 0.0 -layout ???
# mp add-port Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Determine_rows_to_be_deleted_incl_dedup_of_port_1_.in.in0
# mp add-port Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Determine_rows_to_be_deleted_incl_dedup_of_port_1_.in.in1 -singlematch
# mp merge-join Insert_Routine.Join_CSV_File_with_dwh_TA_C_VERTRAG '{vertrags_id}' "${_AB_PROXY_DIR}"'/Join_CSV_File_with_dwh_TA_C_VERTRAG-10.xfr' -max-core 8388608 -limit 0 -ramp 0.0 -layout ???
# mp add-port Insert_Routine.Join_CSV_File_with_dwh_TA_C_VERTRAG.in.in0
# mp add-port Insert_Routine.Join_CSV_File_with_dwh_TA_C_VERTRAG.in.in1 -matchoptional -override-key '{vertrag_id_carmen}'
# mp sort-groups Insert_Routine.Sort_within_Groups_order_by_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id -major-key '{vertrags_id}' -minor-key '{rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id}' -max-core 10485760 -layout ???
# mp db-lookup Insert_Routine.Join_with_dwh_ta_c_vertrag "${BHB_DB}"'/DWH_BHB.dbc' 'select 
# c.rahmenvertrag_id,
# c.dwh_vertrag_id,
# c.dwh_gp_id,
# c.dwh_konto_id,
# c.dwh_tarifgr_id,
# c.vo_kenn,
# c.zv_id,
# c.gueltig_von,
# c.gueltig_bis
# from 
# dwh$ta_c_vertrag c
# where
# c.vertrag_id_carmen (+) = :vertrags_id and
# c.gueltig_bis >= to_date('"'"'20050401'"'"', '"'"'YYYYMMDD'"'"')' "${_AB_PROXY_DIR}"'/Join_with_dwh_ta_c_vertrag-11.xfr' ~null -maximum_matches -1 -commit_number -1 -limit 0 -ramp 0.0 -fixed_size_dml -generate_dml_with_nulls -select -layout ???
# mp local-sort Insert_Routine.Processing_with_sonstige_Positionen_.Sort '{rechnung_datum; rechnung_id; vertrags_id; standardvertrags_id; rech_leistung_id_carm; gueltig_von descending; dwh_vertrag_id descending}' -max-core 100663296 -layout ???
# mp dedup Insert_Routine.Processing_with_sonstige_Positionen_.Dedup_Sorted '{rechnung_datum; rechnung_id; vertrags_id; standardvertrags_id; rech_leistung_id_carm}' -keep first -limit 0 -ramp -1 -check-sort -layout ???
# mp local-sort Insert_Routine.Processing_with_sonstige_Positionen_.Sort_1 '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; dwh_vertrag_id descending; monats_id; debitor_id; abs_grp; dwh_gp_id; dwh_konto_id; dwh_tarifgr_id; vo_kenn; rahmenvertrag_id; zv_id; verkauftes_basisprodukt_id; rechnungvertrag_id; pooling}' -max-core 100663296 -layout ???
mp itable Insert_Routine.dwh_ta_c_vertrag__table_ "${BHB_DB}"'/DWH_BHB.dbc' -select 'select 
rahmenvertrag_id,
vertrag_id_carmen,
dwh_vertrag_id,
dwh_gp_id,
dwh_konto_id,
dwh_tarifgr_id,
vo_kenn,
zv_id,
gueltig_von, 
gueltig_bis
from 
dwh$ta_c_vertrag
where 
gueltig_bis >= to_date('"'"'20050401'"'"', '"'"'YYYYMMDD'"'"') 
and ABLOCAL(dwh$ta_c_vertrag)' -interface api -field_type_preference delimited -layout layout1
mp local-sort Insert_Routine.Sort_by_vertrag_id_carmen '{vertrag_id_carmen}' -max-core 200663296 -layout layout1
mp local-merge Insert_Routine.Merge '{vertrag_id_carmen}' -layout layout2
mp filter Read_and_Preprocessing_File.Read_Filename echo "${BHB_Dateiname}" -layout layout2
mp readfiles Read_and_Preprocessing_File.Read_File "${_AB_PROXY_DIR}"'/Read_File-14.xfr' -limit 0 -ramp 0.0 -file-empty Ignore -file-missing Fail -filename-error Fail -layout layout2
mp copy Read_and_Preprocessing_File.Reformat_Data -layout layout3
mp select-transform Read_and_Preprocessing_File.Split_Data 'kennzeichen == "'"${BHB_Nutzdatensatzkennung}"'"' -limit 0 -ramp 0.0 -layout layout3
mp reformat-transform Read_and_Preprocessing_File.Reformat_Referencerecord -limit 0 -ramp 0.0 -layout layout3
mp add-port Read_and_Preprocessing_File.Reformat_Referencerecord.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_Referencerecord-17.xfr'
mp reformat-transform Splitting_and_Validating_the_reference_data.replace_by_ -limit 0 -ramp 0.0 -layout layout3
mp add-port Splitting_and_Validating_the_reference_data.replace_by_.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/replace_by_-18.xfr'
mp copy Splitting_and_Validating_the_reference_data.Redefine_csv_file_format -layout layout3
mp reformat-transform Splitting_and_Validating_the_reference_data.Reformat_for_DB -limit 0 -ramp 0.0 -layout layout3
mp add-port Splitting_and_Validating_the_reference_data.Reformat_for_DB.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_for_DB-20.xfr'
mp reformat-transform Splitting_and_Validating_the_reference_data.Validate_Records -limit 0 -ramp 0.0 -layout layout3
mp add-port Splitting_and_Validating_the_reference_data.Validate_Records.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Validate_Records-22.xfr'
mp select-transform Aggregation_.Filter_by_Expression 'rech_leistung_id_carm == "RABATT"' -limit 0 -ramp -1 -layout layout3
mp local-sort Aggregation_._rechnung_datum_rechnung_id_standardvertrags_id_vertrags_id_debitor_id_ '{rechnung_datum; rechnung_id; standardvertrags_id; vertrags_id; debitor_id}' -max-core 1073741824 -layout layout3
mp rollup Aggregation_.Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_1 '{rechnung_datum; rechnung_id; standardvertrags_id; vertrags_id; debitor_id}' "${_AB_PROXY_DIR}"'/Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_1-24.xfr' -limit 0 -ramp 0.0 -check-sort -layout layout3
mp gather Aggregation_.Gather -layout layout3
mp local-sort Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id}' -max-core 1073741824 -layout layout3
mp broadcast Replicate -layout layout3
mp merge-join Insert_Routine.Join_with_dwh_ta_c_vertrag_1 '{vertrags_id}' "${_AB_PROXY_DIR}"'/Join_with_dwh_ta_c_vertrag_1-25.xfr' -max-core 8388608 -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Join_with_dwh_ta_c_vertrag_1.in.in0 'is_valid(vertrags_id)'
mp add-port Insert_Routine.Join_with_dwh_ta_c_vertrag_1.in.in1 -matchoptional -override-key '{vertrag_id_carmen}'
mp local-sort Insert_Routine.Sort_by_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id}' -max-core 273741824 -layout layout4
mp broadcast Insert_Routine.Replicate -layout layout4
mp reformat-transform Insert_Routine.Filter_out_where_rpos_geschaeftsform_kenn_S_ -select 'rpos_geschaftsform_kenn != '"'"'S'"'" -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Filter_out_where_rpos_geschaeftsform_kenn_S_.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Filter_out_where_rpos_geschaeftsform_kenn_S_-27.xfr'
mp reformat-transform Insert_Routine.Proof_Historisation_Criterias_Path_1_.Proof_Join_criteriase_gueltig_von_and_gueltig_bis -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Proof_Historisation_Criterias_Path_1_.Proof_Join_criteriase_gueltig_von_and_gueltig_bis.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Proof_Join_criteriase_gueltig_von_and_gueltig_bis-29.xfr'
mp sort-groups Insert_Routine.Proof_Historisation_Criterias_Path_1_.Sort_within_Groups_Sort_by_monats_id_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_abs_grp_prob_vertrag_id_prob_provider_kenn_anz_leistungen_anz_tickets_rpos_geschaftsform_kenn_vas_kenn_kontier_grp_id_gueltig_von_descending -major-key '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id}' -minor-key '{monats_id; rechpos_brutto_eur; rechpos_netto_eur; rechpos_mwst_eur; abs_grp; prob_vertrag_id; prob_provider_kenn; anz_leistungen; anz_tickets; rpos_geschaftsform_kenn; vas_kenn; kontier_grp_id; gueltig_von descending}' -max-core 1073741824 -layout layout4
mp scan Insert_Routine.Proof_Historisation_Criterias_Path_1_.Scan_Mark_valid_historized_datasets '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id; monats_id; rechpos_brutto_eur; rechpos_netto_eur; rechpos_mwst_eur; abs_grp; prob_vertrag_id; prob_provider_kenn; anz_leistungen; anz_tickets; rpos_geschaftsform_kenn; vas_kenn; kontier_grp_id}' "${_AB_PROXY_DIR}"'/Scan_Mark_valid_historized_datasets-31.xfr' -limit 0 -ramp 0.0 -check-sort -layout layout4
mp reformat-transform Insert_Routine.Proof_Historisation_Criterias_Path_1_.Filter_out_invalid_data -select 'valid_flag == 0' -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Proof_Historisation_Criterias_Path_1_.Filter_out_invalid_data.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Filter_out_invalid_data-33.xfr'
mp sort-groups Insert_Routine.Processing_without_sonstige_Positionen_.Sort_within_Groups_Sort_by_gueltig_von_dwh_vertrag_id_descending_ -major-key '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' -minor-key '{gueltig_von alnum null; dwh_vertrag_id alnum null descending}' -max-core 1073741824 -layout layout4
mp scan Insert_Routine.Processing_without_sonstige_Positionen_.Scan_Ranking_over_gueltig_von_dwh_vertrag_id_desc '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' "${_AB_PROXY_DIR}"'/Scan_Ranking_over_gueltig_von_dwh_vertrag_id_desc-35.xfr' -limit 0 -ramp 0.0 -check-sort -layout layout4
mp reformat-transform Insert_Routine.Processing_without_sonstige_Positionen_.Filter_out_where_rankindex_1 -select 'rankindex == 1' -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Processing_without_sonstige_Positionen_.Filter_out_where_rankindex_1.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Filter_out_where_rpos_geschaeftsform_kenn_S_-27.xfr'
mp sort-groups Insert_Routine.Processing_without_sonstige_Positionen_.Sort_within_Groups_Sort_by_dwh_vertrag_id_prob_vertrag_id_monats_id_debitor_id_abs_grp_prob_provider_kenn_dwh_vertrag_id_dwh_gp_id_dwh_konto_id_vo_kenn_rahmenvertrag_id_dwh_tarifgr_id_rpos_geschaftsform_kenn_vas_kenn -major-key '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' -minor-key '{dwh_vertrag_id descending; prob_vertrag_id; monats_id; debitor_id; abs_grp; prob_provider_kenn; dwh_gp_id; dwh_konto_id; vo_kenn; rahmenvertrag_id; dwh_tarifgr_id; rpos_geschaftsform_kenn; vas_kenn}' -max-core 1073741824 -layout layout4
mp rollup Insert_Routine.Processing_without_sonstige_Positionen_.Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_anz_leistungen_anz_tickets '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; dwh_vertrag_id descending; prob_vertrag_id; monats_id; debitor_id; abs_grp; prob_provider_kenn; dwh_gp_id; dwh_konto_id; vo_kenn; rahmenvertrag_id; dwh_tarifgr_id; rpos_geschaftsform_kenn; vas_kenn}' "${_AB_PROXY_DIR}"'/Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_anz_leistungen_anz_tickets-37.xfr' -limit 0 -ramp 0.0 -check-sort -layout layout4
mp reformat-transform Insert_Routine.Processing_without_sonstige_Positionen_.Decode_rpos_geschaeftsform_kenn -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Processing_without_sonstige_Positionen_.Decode_rpos_geschaeftsform_kenn.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Decode_rpos_geschaeftsform_kenn-38.xfr'
mp select-transform Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Select_Factoring_Rechnungen_ 'rpos_geschaftsform_kenn == '"'"'F'"'" -limit 0 -ramp 0.0 -layout layout4
mp reformat-transform Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Reformat_for_insert_Factoring_Rechnungen_ -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Reformat_for_insert_Factoring_Rechnungen_.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_for_insert_Factoring_Rechnungen_-40.xfr'
mp select-transform Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Select_Factoring_Gutschriften_ 'rpos_geschaftsform_kenn == '"'"'G'"'" -limit 0 -ramp 0.0 -layout layout4
mp reformat-transform Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Reformat_for_insert_Factoring_Gutschriften_ -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Reformat_for_insert_Factoring_Gutschriften_.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_for_insert_Factoring_Gutschriften_-42.xfr'
mp select-transform Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Select_Reselling_ 'rpos_geschaftsform_kenn == '"'"'R'"'" -limit 0 -ramp 0.0 -layout layout4
mp reformat-transform Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Reformat_for_insert_Reselling_ -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Reformat_for_insert_Reselling_.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_for_insert_Factoring_Rechnungen_-40.xfr'
mp reformat-transform Insert_Routine.Proof_Historisation_Criterias_Path2_.Proof_Join_criterias_gueltig_von_and_gueltig_bis -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Proof_Historisation_Criterias_Path2_.Proof_Join_criterias_gueltig_von_and_gueltig_bis.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Proof_Join_criterias_gueltig_von_and_gueltig_bis-45.xfr'
mp sort-groups Insert_Routine.Proof_Historisation_Criterias_Path2_.Sort_within_Groups_Sort_by_kontier_grp_id_monats_id_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_abs_grp_pooling_rechnungvertrag_id_verkauftes_basisprodukt_id_gueltig_von_descending -major-key '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id}' -minor-key '{kontier_grp_id; monats_id; rechpos_brutto_eur; rechpos_netto_eur; rechpos_mwst_eur; abs_grp; pooling; rechnungvertrag_id; verkauftes_basisprodukt_id; gueltig_von descending}' -max-core 1073741824 -layout layout4
mp scan Insert_Routine.Proof_Historisation_Criterias_Path2_.Scan_Mark_valid_historized_datasets '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; debitor_id; kontier_grp_id; monats_id; rechpos_brutto_eur; rechpos_netto_eur; rechpos_mwst_eur; abs_grp; pooling; rechnungvertrag_id; verkauftes_basisprodukt_id}' "${_AB_PROXY_DIR}"'/Scan_Mark_valid_historized_datasets-31.xfr' -limit 0 -ramp 0.0 -check-sort -layout layout4
mp reformat-transform Insert_Routine.Proof_Historisation_Criterias_Path2_.Filter_out_invalid_data -select 'valid_flag == 0' -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Proof_Historisation_Criterias_Path2_.Filter_out_invalid_data.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Filter_out_invalid_data-33.xfr'
mp sort-groups Insert_Routine.Processing_with_sonstige_Positionen_.Sort_within_Groups_Sort_by_gueltig_von_descending_dwh_vertrag_id_descending_ -major-key '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' -minor-key '{gueltig_von alnum null descending; dwh_vertrag_id alnum null descending}' -max-core 1073741824 -layout layout4
mp scan Insert_Routine.Processing_with_sonstige_Positionen_.Scan_Ranking_over_gueltig_von_desc_dwh_vertrag_id_desc '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' "${_AB_PROXY_DIR}"'/Scan_Ranking_over_gueltig_von_desc_dwh_vertrag_id_desc-49.xfr' -limit 0 -ramp 0.0 -check-sort -layout layout4
mp reformat-transform Insert_Routine.Processing_with_sonstige_Positionen_.Filter_out_where_rankindex_1 -select 'rankindex == 1' -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Processing_with_sonstige_Positionen_.Filter_out_where_rankindex_1.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Filter_out_where_rpos_geschaeftsform_kenn_S_-27.xfr'
mp sort-groups Insert_Routine.Processing_with_sonstige_Positionen_.Sort_within_Groups_Sort_by_dwh_vertrag_id_monats_id_debitor_id_abs_grp_dwh_gp_id_dwh_konto_id_dwh_tarifgr_id_vo_kenn_rahmenvertrag_id_zv_id_verkauftes_basisprodukt_id_rechnungvertrag_id_pooling -major-key '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' -minor-key '{dwh_vertrag_id descending; monats_id; debitor_id; abs_grp; dwh_gp_id; dwh_konto_id; dwh_tarifgr_id; vo_kenn; rahmenvertrag_id; zv_id; verkauftes_basisprodukt_id; rechnungvertrag_id; pooling}' -max-core 1073741824 -layout layout4
mp rollup Insert_Routine.Processing_with_sonstige_Positionen_.Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; dwh_vertrag_id descending; monats_id; debitor_id; abs_grp; dwh_gp_id; dwh_konto_id; dwh_tarifgr_id; vo_kenn; rahmenvertrag_id; zv_id; verkauftes_basisprodukt_id; rechnungvertrag_id; pooling}' "${_AB_PROXY_DIR}"'/Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur-51.xfr' -limit 0 -ramp 0.0 -check-sort -layout layout4
mp select-transform Insert_Routine.Processing_with_sonstige_Positionen_.Select_Positionen_auf_Debitorenebene_temporary_Data_ 'typ == '"'"'T'"'" -limit 0 -ramp 0.0 -layout layout4
mp reformat-transform Insert_Routine.Processing_with_sonstige_Positionen_.Reformat_for_insert_fact_data_ -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Processing_with_sonstige_Positionen_.Reformat_for_insert_fact_data_.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_for_insert_fact_data_-53.xfr'
mp reformat-transform Insert_Routine.Processing_with_sonstige_Positionen_.Reformat_for_insert_temporary_data_ -limit 0 -ramp 0.0 -layout layout4
mp add-port Insert_Routine.Processing_with_sonstige_Positionen_.Reformat_for_insert_temporary_data_.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_for_insert_temporary_data_-55.xfr'
mp reformat-transform Reformat_rechnung_datum_to_datetime_for_Delete -limit 0 -ramp 0.0 -layout layout3
mp add-port Reformat_rechnung_datum_to_datetime_for_Delete.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Filter_out_where_rpos_geschaeftsform_kenn_S_-27.xfr'
mp dedup Delete_Routine.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm}' -keep first -limit 0 -ramp 0.0 -check-sort -layout layout3
mp broadcast Delete_Routine.Replicate -layout layout3
mp reformat-transform Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Reformat_for_delete -limit 0 -ramp 0.0 -layout layout2
mp add-port Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Reformat_for_delete.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_for_delete-58.xfr'
mp db-update Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Delete_rows_from_DWH_TA_F_GPOS_FACT_CARM "${BHB_DB}"'/DWH_BHB.dbc' "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_F_GPOS_FACT_CARM-60.sql' ~null -interface api -new_db_update -no_actions_ok -layout layout2
mp db-update Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Delete_rows_from_DWH_TA_F_RPOS_CARM_2 "${BHB_DB}"'/DWH_BHB.dbc' "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_F_RPOS_CARM_2-61.sql' ~null -interface api -new_db_update -no_actions_ok -layout layout3
mp reformat-transform Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Reformat_for_delete -limit 0 -ramp 0.0 -layout layout2
mp add-port Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Reformat_for_delete.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_for_delete-58.xfr'
mp db-update Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Delete_rows_from_DWH_TA_F_RPOS_FACT_CARM "${BHB_DB}"'/DWH_BHB.dbc' "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_F_RPOS_FACT_CARM-62.sql' ~null -interface api -new_db_update -no_actions_ok -layout layout2
mp reformat-transform Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Reformat -limit 0 -ramp 0.0 -layout layout2
mp add-port Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Reformat.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_for_delete-58.xfr'
mp db-update Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Delete_rows_from_DWH_TA_F_RPOS_RESELLING_CARM "${BHB_DB}"'/DWH_BHB.dbc' "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_F_RPOS_RESELLING_CARM-63.sql' ~null -interface api -new_db_update -no_actions_ok -layout layout2
mp reformat-transform Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Reformat -limit 0 -ramp 0.0 -layout layout2
mp add-port Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Reformat.out.out0
mp db-update Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Delete_rows_from_DWH_TA_T_RPOS_CARM "${BHB_DB}"'/DWH_BHB.dbc' "${_AB_PROXY_DIR}"'/Delete_rows_from_DWH_TA_T_RPOS_CARM-65.sql' ~null -interface api -new_db_update -no_actions_ok -layout layout2
mp select-transform Read_and_Preprocessing_File.Split_Metadata 'kennzeichen == "'"${BHB_Endedatensatzkennung}"'"' -limit 0 -ramp 0.0 -layout layout3
mp copy Process_Enderecord.Format_Enderecord -layout layout3
mp broadcast Process_Enderecord.Replicate_Enderecord -layout layout3
mp reformat-transform Process_Enderecord.Reformat_Enderecord_for_Processing -limit 0 -ramp 0.0 -layout layout3
mp add-port Process_Enderecord.Reformat_Enderecord_for_Processing.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_Enderecord_for_Processing-67.xfr'
mp reformat-transform Reformat_for_DB_and_Filter_out_where_Kompl_Kennzeichen_L_ -select 'string_substring(bemerkung, 18,1)=='"'"'L'"'" -limit 0 -ramp 0.0 -layout layout3
mp add-port Reformat_for_DB_and_Filter_out_where_Kompl_Kennzeichen_L_.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_for_DB_and_Filter_out_where_Kompl_Kennzeichen_L_-68.xfr'
mp db-update Update_Insert_DWH_TA_K_RECH_ABSGRP "${BHB_DB}"'/DWH_BHB.dbc' "${_AB_PROXY_DIR}"'/Update_Insert_DWH_TA_K_RECH_ABSGRP-70.sql' "${_AB_PROXY_DIR}"'/Update_Insert_DWH_TA_K_RECH_ABSGRP-71.sql' -interface api -new_db_update -layout layout3
mp reformat-transform Process_Enderecord.Reformat_Enderecord_for_Update -limit 0 -ramp 0.0 -layout layout3
mp add-port Process_Enderecord.Reformat_Enderecord_for_Update.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_Enderecord_for_Update-72.xfr'
mp db-update Process_Enderecord.Update_DWH_TA_K_MELDUNGEN "${BHB_DB}"'/DWH_BHB.dbc' "${_AB_PROXY_DIR}"'/Update_DWH_TA_K_MELDUNGEN-74.sql' ~null -interface api -new_db_update -layout layout3
# mp reformat-transform Splitting_and_Validating_the_reference_data.Reformat_fur_testzwecke -limit 0 -ramp 0.0 -layout ???
# mp add-port Splitting_and_Validating_the_reference_data.Reformat_fur_testzwecke.out.out0 ${_AB_PROXY_DIR:+"$_AB_PROXY_DIR"}'/Reformat_fur_testzwecke-75.xfr'
# mp otable Splitting_and_Validating_the_reference_data.Output_Table_TESTDATEN__table_ "${BHB_DB}"'/DWH_BHB.dbc' -flags wronly,append -table TESTDATEN -interface api -field_type_preference delimited -layout layout4
mp checkpoint 0

# Components in phase 1:
mp otable Insert_Routine.Processing_with_sonstige_Positionen_.DWH_TA_F_RPOS_CARM__table_ "${BHB_DB}"'/DWH_BHB.dbc' -flags wronly,append -table 'DWH$TA_F_RPOS_CARM' -direct -interface utility -field_type_preference delimited -num_errors 0 -rows_per_commit 1000 -layout layout4
mp otable Insert_Routine.Processing_with_sonstige_Positionen_.DWH_TA_T_RPOS_CARM__table_ "${BHB_DB}"'/DWH_BHB.dbc' -flags wronly,append -table 'DWH$TA_T_RPOS_CARM' -direct -interface utility -field_type_preference delimited -num_errors 0 -rows_per_commit 1000 -layout layout4
mp otable Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.DWH_TA_F_GPOS_FACT_CARM__table_ "${BHB_DB}"'/DWH_BHB.dbc' -flags wronly,append -table 'DWH$TA_F_GPOS_FACT_CARM' -direct -interface utility -field_type_preference delimited -num_errors 0 -rows_per_commit 1000 -layout layout4
mp otable Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.DWH_TA_F_RPOS_FACT_CARM__table_ "${BHB_DB}"'/DWH_BHB.dbc' -flags wronly,append -table 'DWH$TA_F_RPOS_FACT_CARM' -direct -interface utility -field_type_preference delimited -num_errors 0 -rows_per_commit 1000 -layout layout4
mp otable Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.DWH_TA_F_RPOS_RESELLING_CARM__table_ "${BHB_DB}"'/DWH_BHB.dbc' -flags wronly,append -table 'DWH$TA_F_RPOS_RESELLING_CARM' -direct -interface utility -field_type_preference delimited -num_errors 0 -rows_per_commit 1000 -layout layout4
# mp local-sort Insert_Routine.Processing_without_sonstige_Positionen_.Sort '{rechnung_datum; rechnung_id; vertrags_id; standardvertrags_id; rech_leistung_id_carm; gueltig_von descending; dwh_vertrag_id descending}' -max-core 100663296 -layout ???
# mp dedup Insert_Routine.Processing_without_sonstige_Positionen_.Dedup_Sorted '{rechnung_datum; rechnung_id; vertrags_id; standardvertrags_id; rech_leistung_id_carm}' -keep first -limit 0 -ramp -1 -check-sort -layout ???
# mp local-sort Insert_Routine.Processing_without_sonstige_Positionen_._vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_dwh_vertrag_id_descending_prob_vertrag_id_monats_id_debitor_id_abs_grp_prob_provider_kenn_dwh_gp_id_dwh_konto_id_vo_kenn_rahmenvertrag_id_dwh_tarifgr_id_rpos_geschaftsform_kenn_vas_kenn_ '{vertrags_id; rechnung_id; rechnung_datum; standardvertrags_id; rech_leistung_id_carm; dwh_vertrag_id descending; prob_vertrag_id; monats_id; debitor_id; abs_grp; prob_provider_kenn; dwh_gp_id; dwh_konto_id; vo_kenn; rahmenvertrag_id; dwh_tarifgr_id; rpos_geschaftsform_kenn; vas_kenn}' -max-core 100663296 -layout ???

# Flows for Entire Graph:
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Flow_1 Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.DWH_TA_F_GPOS_FACT_CARM_2__table_.read Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.in -metadata metadata16
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Flow_2 Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.out Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.in -metadata metadata16
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Flow_7 Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.out Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Sort_within_Groups_Sort_over_rech_leistung_id_carm.in -metadata metadata16
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Flow_5 Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.DWH_TA_F_GPOS_FACT_CARM__table_.read Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm.in -metadata metadata16
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Flow_6 Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm.out Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm.in -metadata metadata16
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Flow_3 Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm.out Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Determine_rows_to_be_deleted.in.in0 -metadata metadata16
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Flow_4 Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Determine_rows_to_be_deleted.out Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Dedup_Sorted_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id.in -metadata metadata26
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Flow_1 Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.DWH_TA_F_RPOS_CARM_2__table_.read Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Flow_2 Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Flow_3 Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Sort_within_Groups_Sort_over_rech_leistung_id_carm.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Flow_5 Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.DWH_TA_F_RPOS_CARM__table_.read Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Flow_6 Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Determine_rows_to_be_deleted.in.in0
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Flow_4 Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Determine_rows_to_be_deleted.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Dedup_Sorted_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id.in -metadata metadata26
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Flow_7 Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Dedup_Sorted_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Delete_rows_from_DWH_TA_F_RPOS_CARM.in -metadata metadata26
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Flow_8 Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Join_with_DB.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Filter_by_Expression.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Flow_10 Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Filter_by_Expression.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Dedup_Sorted_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_1.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Flow_9 Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Dedup_Sorted_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_1.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Delete_rows_from_DWH_TA_F_RPOS_CARM_1.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Flow_1 Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.DWH_TA_F_RPOS_FACT_CARM_2__table_.read Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Flow_2 Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Flow_3 Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Sort_within_Groups_Sort_over_rech_leistung_id_carm.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Flow_5 Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.DWH_TA_F_RPOS_FACT_CARM__table_.read Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Flow_6 Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Determine_rows_to_be_deleted.in.in0
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Flow_8 Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Join_with_DB_Determine_rows_to_be_deleted.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Sort_over_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id.in -metadata metadata26
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Flow_4 Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Sort_over_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Dedup_Sorted_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id.in -metadata metadata26
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Flow_1 Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.DWH_TA_F_RPOS_RESELLING_CARM_1__table_.read Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Flow_2 Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Flow_3 Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_debitor_id.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Sort_within_Groups_Sort_over_rech_leistung_id_carm.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Flow_5 Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.DWH_TA_F_RPOS_RESELLING_CARM__table_.read Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm.in
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Flow_6 Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Determine_rows_to_be_deleted.in.in0
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Flow_7 Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Join_with_DB_Determine_rows_to_be_deleted.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Sort_over_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id.in -metadata metadata26
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Flow_4 Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Sort_over_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Dedup_Sorted_over_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id.in -metadata metadata26
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Flow_1 Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.DWH_TA_T_RPOS_CARM_2__table_.read Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id.in -metadata metadata24
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Flow_2 Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id.out Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id.in -metadata metadata24
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Flow_9 Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id.out Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Determine_rows_to_be_deleted.in.in0 -metadata metadata24
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Flow_4 Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Determine_rows_to_be_deleted.out Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_rechnung_id_rechnung_datum_debitor_id.in -metadata metadata27
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Flow_6 Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_rechnung_id_rechnung_datum_debitor_id.out Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Dedup_Sorted_over_rechnung_id_rechnung_datum_debitor_id.in -metadata metadata27
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Flow_8 Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.DWH_TA_T_RPOS_CARM__table_.read Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_rechnung_id_rechnung_datum_debitor_id_1.in -metadata metadata24
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Flow_3 Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_rechnung_id_rechnung_datum_debitor_id_1.out Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Dedup_Sorted_over_rechnung_id_rechnung_datum_debitor_id_1.in -metadata metadata24
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Flow_12 Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Join_with_DB_Determine_rows_to_be_deleted.out Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_rechnung_id_rechnung_datum_debitor_id_3.in -metadata metadata27
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Flow_11 Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_rechnung_id_rechnung_datum_debitor_id_3.out Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Dedup_Sorted_over_rechnung_id_rechnung_datum_debitor_id_2.in -metadata metadata27
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Flow_7 Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Dedup_Sorted_over_rechnung_id_rechnung_datum_debitor_id_1.out Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Determine_rows_to_be_deleted_incl_dedup_of_port_1_.in.in0 -metadata metadata24
# mp straight-flow Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Flow_10 Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Sort_by_rechnung_id_rechnung_datum_debitor_id_2.out Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Determine_rows_to_be_deleted_incl_dedup_of_port_1_.in.in1
# mp straight-flow Insert_Routine.Flow_4 Insert_Routine.Join_CSV_File_with_dwh_TA_C_VERTRAG.out Insert_Routine.Sort_within_Groups_order_by_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id.in
# mp straight-flow Insert_Routine.Processing_with_sonstige_Positionen_.Flow_7 Insert_Routine.Processing_with_sonstige_Positionen_.Sort.out Insert_Routine.Processing_with_sonstige_Positionen_.Dedup_Sorted.in
# mp straight-flow Insert_Routine.Processing_with_sonstige_Positionen_.Flow_8 Insert_Routine.Processing_with_sonstige_Positionen_.Dedup_Sorted.out Insert_Routine.Processing_with_sonstige_Positionen_.Sort_1.in
mp straight-flow Insert_Routine.Flow_2 Insert_Routine.dwh_ta_c_vertrag__table_.read Insert_Routine.Sort_by_vertrag_id_carmen.in -metadata metadata1
mp fan-in-flow Insert_Routine.Flow_13 Insert_Routine.Sort_by_vertrag_id_carmen.out Insert_Routine.Merge.in -metadata metadata1
mp straight-flow Read_and_Preprocessing_File.Flow_4 Read_and_Preprocessing_File.Read_Filename.out Read_and_Preprocessing_File.Read_File.in -metadata metadata2
mp straight-flow Read_and_Preprocessing_File.Flow_1 Read_and_Preprocessing_File.Read_File.out Read_and_Preprocessing_File.Reformat_Data.in -metadata metadata3
mp straight-flow Read_and_Preprocessing_File.Flow_3 Read_and_Preprocessing_File.Reformat_Data.out Read_and_Preprocessing_File.Split_Data.in -metadata metadata4
mp straight-flow Read_and_Preprocessing_File.Flow_6 Read_and_Preprocessing_File.Split_Data.out Read_and_Preprocessing_File.Reformat_Referencerecord.in -metadata metadata4
mp straight-flow Flow_1 Read_and_Preprocessing_File.Reformat_Referencerecord.out.out0 Splitting_and_Validating_the_reference_data.replace_by_.in -metadata metadata4
mp straight-flow Splitting_and_Validating_the_reference_data.Flow_6 Splitting_and_Validating_the_reference_data.replace_by_.out.out0 Splitting_and_Validating_the_reference_data.Redefine_csv_file_format.in -metadata metadata4
mp straight-flow Splitting_and_Validating_the_reference_data.Flow_1 Splitting_and_Validating_the_reference_data.Redefine_csv_file_format.out Splitting_and_Validating_the_reference_data.Reformat_for_DB.in -metadata metadata5
mp straight-flow Splitting_and_Validating_the_reference_data.Flow_5 Splitting_and_Validating_the_reference_data.Reformat_for_DB.out.out0 Splitting_and_Validating_the_reference_data.Validate_Records.in -metadata metadata6
mp straight-flow Flow_10 Splitting_and_Validating_the_reference_data.Validate_Records.out.out0 Aggregation_.Filter_by_Expression.in -metadata metadata7
mp straight-flow Aggregation_.Flow_10 Aggregation_.Filter_by_Expression.out Aggregation_._rechnung_datum_rechnung_id_standardvertrags_id_vertrags_id_debitor_id_.in -metadata metadata7
mp straight-flow Aggregation_.Flow_11 Aggregation_._rechnung_datum_rechnung_id_standardvertrags_id_vertrags_id_debitor_id_.out Aggregation_.Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_1.in -metadata metadata7
mp straight-flow Aggregation_.Flow_12 Aggregation_.Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_1.out Aggregation_.Gather.in -metadata metadata7
mp straight-flow Aggregation_.Flow_14 Aggregation_.Filter_by_Expression.deselect Aggregation_.Gather.in -metadata metadata7
mp straight-flow Flow_2 Aggregation_.Gather.out Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id.in -metadata metadata7
mp straight-flow Flow_8 Sort_by_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id.out Replicate.in -metadata metadata7
mp straight-flow Flow_5 Replicate.out Insert_Routine.Join_with_dwh_ta_c_vertrag_1.in.in0 -metadata metadata7 -buffer
mp straight-flow Insert_Routine.Flow_14 Insert_Routine.Merge.out Insert_Routine.Join_with_dwh_ta_c_vertrag_1.in.in1 -metadata metadata1 -buffer
mp straight-flow Insert_Routine.Flow_10 Insert_Routine.Join_with_dwh_ta_c_vertrag_1.out Insert_Routine.Sort_by_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id.in -metadata metadata8
mp straight-flow Insert_Routine.Flow_9 Insert_Routine.Sort_by_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_debitor_id.out Insert_Routine.Replicate.in -metadata metadata8
mp straight-flow Insert_Routine.Flow_7 Insert_Routine.Replicate.out Insert_Routine.Filter_out_where_rpos_geschaeftsform_kenn_S_.in -metadata metadata8
mp straight-flow Insert_Routine.Flow_6 Insert_Routine.Filter_out_where_rpos_geschaeftsform_kenn_S_.out.out0 Insert_Routine.Proof_Historisation_Criterias_Path_1_.Proof_Join_criteriase_gueltig_von_and_gueltig_bis.in -metadata metadata9
mp straight-flow Insert_Routine.Proof_Historisation_Criterias_Path_1_.Flow_1 Insert_Routine.Proof_Historisation_Criterias_Path_1_.Proof_Join_criteriase_gueltig_von_and_gueltig_bis.out.out0 Insert_Routine.Proof_Historisation_Criterias_Path_1_.Sort_within_Groups_Sort_by_monats_id_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_abs_grp_prob_vertrag_id_prob_provider_kenn_anz_leistungen_anz_tickets_rpos_geschaftsform_kenn_vas_kenn_kontier_grp_id_gueltig_von_descending.in -metadata metadata10
mp straight-flow Insert_Routine.Proof_Historisation_Criterias_Path_1_.Flow_2 Insert_Routine.Proof_Historisation_Criterias_Path_1_.Sort_within_Groups_Sort_by_monats_id_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_abs_grp_prob_vertrag_id_prob_provider_kenn_anz_leistungen_anz_tickets_rpos_geschaftsform_kenn_vas_kenn_kontier_grp_id_gueltig_von_descending.out Insert_Routine.Proof_Historisation_Criterias_Path_1_.Scan_Mark_valid_historized_datasets.in -metadata metadata10
mp straight-flow Insert_Routine.Proof_Historisation_Criterias_Path_1_.Flow_3 Insert_Routine.Proof_Historisation_Criterias_Path_1_.Scan_Mark_valid_historized_datasets.out Insert_Routine.Proof_Historisation_Criterias_Path_1_.Filter_out_invalid_data.in -metadata metadata11
mp straight-flow Insert_Routine.Flow_1 Insert_Routine.Proof_Historisation_Criterias_Path_1_.Filter_out_invalid_data.out.out0 Insert_Routine.Processing_without_sonstige_Positionen_.Sort_within_Groups_Sort_by_gueltig_von_dwh_vertrag_id_descending_.in -metadata metadata12
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Flow_7 Insert_Routine.Processing_without_sonstige_Positionen_.Sort_within_Groups_Sort_by_gueltig_von_dwh_vertrag_id_descending_.out Insert_Routine.Processing_without_sonstige_Positionen_.Scan_Ranking_over_gueltig_von_dwh_vertrag_id_desc.in -metadata metadata12
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Flow_15 Insert_Routine.Processing_without_sonstige_Positionen_.Scan_Ranking_over_gueltig_von_dwh_vertrag_id_desc.out Insert_Routine.Processing_without_sonstige_Positionen_.Filter_out_where_rankindex_1.in -metadata metadata13
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Flow_6 Insert_Routine.Processing_without_sonstige_Positionen_.Filter_out_where_rankindex_1.out.out0 Insert_Routine.Processing_without_sonstige_Positionen_.Sort_within_Groups_Sort_by_dwh_vertrag_id_prob_vertrag_id_monats_id_debitor_id_abs_grp_prob_provider_kenn_dwh_vertrag_id_dwh_gp_id_dwh_konto_id_vo_kenn_rahmenvertrag_id_dwh_tarifgr_id_rpos_geschaftsform_kenn_vas_kenn.in -metadata metadata13
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Flow_5 Insert_Routine.Processing_without_sonstige_Positionen_.Sort_within_Groups_Sort_by_dwh_vertrag_id_prob_vertrag_id_monats_id_debitor_id_abs_grp_prob_provider_kenn_dwh_vertrag_id_dwh_gp_id_dwh_konto_id_vo_kenn_rahmenvertrag_id_dwh_tarifgr_id_rpos_geschaftsform_kenn_vas_kenn.out Insert_Routine.Processing_without_sonstige_Positionen_.Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_anz_leistungen_anz_tickets.in -metadata metadata13
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Flow_1 Insert_Routine.Processing_without_sonstige_Positionen_.Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_anz_leistungen_anz_tickets.out Insert_Routine.Processing_without_sonstige_Positionen_.Decode_rpos_geschaeftsform_kenn.in -metadata metadata13
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Flow_2 Insert_Routine.Processing_without_sonstige_Positionen_.Decode_rpos_geschaeftsform_kenn.out.out0 Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Select_Factoring_Rechnungen_.in -metadata metadata14
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Flow_1 Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Select_Factoring_Rechnungen_.out Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Reformat_for_insert_Factoring_Rechnungen_.in -metadata metadata14
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Flow_5 Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Select_Factoring_Rechnungen_.deselect Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Select_Factoring_Gutschriften_.in -metadata metadata14
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Flow_4 Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Select_Factoring_Gutschriften_.out Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Reformat_for_insert_Factoring_Gutschriften_.in -metadata metadata14
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Flow_6 Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Select_Factoring_Gutschriften_.deselect Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Select_Reselling_.in -metadata metadata14
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Flow_8 Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Select_Reselling_.out Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Reformat_for_insert_Reselling_.in -metadata metadata14
mp straight-flow Insert_Routine.Flow_8 Insert_Routine.Replicate.out Insert_Routine.Proof_Historisation_Criterias_Path2_.Proof_Join_criterias_gueltig_von_and_gueltig_bis.in -metadata metadata8
mp straight-flow Insert_Routine.Proof_Historisation_Criterias_Path2_.Flow_1 Insert_Routine.Proof_Historisation_Criterias_Path2_.Proof_Join_criterias_gueltig_von_and_gueltig_bis.out.out0 Insert_Routine.Proof_Historisation_Criterias_Path2_.Sort_within_Groups_Sort_by_kontier_grp_id_monats_id_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_abs_grp_pooling_rechnungvertrag_id_verkauftes_basisprodukt_id_gueltig_von_descending.in -metadata metadata18
mp straight-flow Insert_Routine.Proof_Historisation_Criterias_Path2_.Flow_2 Insert_Routine.Proof_Historisation_Criterias_Path2_.Sort_within_Groups_Sort_by_kontier_grp_id_monats_id_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur_abs_grp_pooling_rechnungvertrag_id_verkauftes_basisprodukt_id_gueltig_von_descending.out Insert_Routine.Proof_Historisation_Criterias_Path2_.Scan_Mark_valid_historized_datasets.in -metadata metadata18
mp straight-flow Insert_Routine.Proof_Historisation_Criterias_Path2_.Flow_3 Insert_Routine.Proof_Historisation_Criterias_Path2_.Scan_Mark_valid_historized_datasets.out Insert_Routine.Proof_Historisation_Criterias_Path2_.Filter_out_invalid_data.in -metadata metadata19
mp straight-flow Insert_Routine.Flow_5 Insert_Routine.Proof_Historisation_Criterias_Path2_.Filter_out_invalid_data.out.out0 Insert_Routine.Processing_with_sonstige_Positionen_.Sort_within_Groups_Sort_by_gueltig_von_descending_dwh_vertrag_id_descending_.in -metadata metadata20
mp straight-flow Insert_Routine.Processing_with_sonstige_Positionen_.Flow_12 Insert_Routine.Processing_with_sonstige_Positionen_.Sort_within_Groups_Sort_by_gueltig_von_descending_dwh_vertrag_id_descending_.out Insert_Routine.Processing_with_sonstige_Positionen_.Scan_Ranking_over_gueltig_von_desc_dwh_vertrag_id_desc.in -metadata metadata20
mp straight-flow Insert_Routine.Processing_with_sonstige_Positionen_.Flow_16 Insert_Routine.Processing_with_sonstige_Positionen_.Scan_Ranking_over_gueltig_von_desc_dwh_vertrag_id_desc.out Insert_Routine.Processing_with_sonstige_Positionen_.Filter_out_where_rankindex_1.in -metadata metadata21
mp straight-flow Insert_Routine.Processing_with_sonstige_Positionen_.Flow_9 Insert_Routine.Processing_with_sonstige_Positionen_.Filter_out_where_rankindex_1.out.out0 Insert_Routine.Processing_with_sonstige_Positionen_.Sort_within_Groups_Sort_by_dwh_vertrag_id_monats_id_debitor_id_abs_grp_dwh_gp_id_dwh_konto_id_dwh_tarifgr_id_vo_kenn_rahmenvertrag_id_zv_id_verkauftes_basisprodukt_id_rechnungvertrag_id_pooling.in -metadata metadata21
mp straight-flow Insert_Routine.Processing_with_sonstige_Positionen_.Flow_6 Insert_Routine.Processing_with_sonstige_Positionen_.Sort_within_Groups_Sort_by_dwh_vertrag_id_monats_id_debitor_id_abs_grp_dwh_gp_id_dwh_konto_id_dwh_tarifgr_id_vo_kenn_rahmenvertrag_id_zv_id_verkauftes_basisprodukt_id_rechnungvertrag_id_pooling.out Insert_Routine.Processing_with_sonstige_Positionen_.Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur.in -metadata metadata21
mp straight-flow Insert_Routine.Processing_with_sonstige_Positionen_.Flow_1 Insert_Routine.Processing_with_sonstige_Positionen_.Rollup_sum_of_rechpos_brutto_eur_rechpos_netto_eur_rechpos_mwst_eur.out Insert_Routine.Processing_with_sonstige_Positionen_.Select_Positionen_auf_Debitorenebene_temporary_Data_.in -metadata metadata22
mp straight-flow Insert_Routine.Processing_with_sonstige_Positionen_.Flow_5 Insert_Routine.Processing_with_sonstige_Positionen_.Select_Positionen_auf_Debitorenebene_temporary_Data_.deselect Insert_Routine.Processing_with_sonstige_Positionen_.Reformat_for_insert_fact_data_.in -metadata metadata22
mp straight-flow Insert_Routine.Processing_with_sonstige_Positionen_.Flow_3 Insert_Routine.Processing_with_sonstige_Positionen_.Select_Positionen_auf_Debitorenebene_temporary_Data_.out Insert_Routine.Processing_with_sonstige_Positionen_.Reformat_for_insert_temporary_data_.in -metadata metadata22
mp straight-flow Flow_9 Replicate.out Reformat_rechnung_datum_to_datetime_for_Delete.in -metadata metadata7
mp straight-flow Flow_3 Reformat_rechnung_datum_to_datetime_for_Delete.out.out0 Delete_Routine.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm.in -metadata metadata25
mp straight-flow Delete_Routine.Flow_6 Delete_Routine.Dedup_Sorted_vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm.out Delete_Routine.Replicate.in -metadata metadata25
mp straight-flow Delete_Routine.Flow_3 Delete_Routine.Replicate.out Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Reformat_for_delete.in -metadata metadata25
mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Flow_8 Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Reformat_for_delete.out.out0 Delete_Routine.Delete_from_DWH_TA_F_GPOS_FACT_CARM.Delete_rows_from_DWH_TA_F_GPOS_FACT_CARM.in -metadata metadata26
mp straight-flow Delete_Routine.Flow_1 Delete_Routine.Replicate.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_CARM.Delete_rows_from_DWH_TA_F_RPOS_CARM_2.in -metadata metadata25
mp straight-flow Delete_Routine.Flow_2 Delete_Routine.Replicate.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Reformat_for_delete.in -metadata metadata25
mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Flow_7 Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Reformat_for_delete.out.out0 Delete_Routine.Delete_from_DWH_TA_F_RPOS_FACT_CARM.Delete_rows_from_DWH_TA_F_RPOS_FACT_CARM.in -metadata metadata26
mp straight-flow Delete_Routine.Flow_4 Delete_Routine.Replicate.out Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Reformat.in -metadata metadata25
mp straight-flow Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Flow_8 Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Reformat.out.out0 Delete_Routine.Delete_from_DWH_TA_F_RPOS_RESELLING_CARM.Delete_rows_from_DWH_TA_F_RPOS_RESELLING_CARM.in -metadata metadata26
mp straight-flow Delete_Routine.Flow_5 Delete_Routine.Replicate.out Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Reformat.in -metadata metadata25
mp straight-flow Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Flow_5 Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Reformat.out.out0 Delete_Routine.Delete_from_DWH_TA_T_RPOS_CARM.Delete_rows_from_DWH_TA_T_RPOS_CARM.in -metadata metadata27
mp straight-flow Read_and_Preprocessing_File.Flow_2 Read_and_Preprocessing_File.Split_Data.deselect Read_and_Preprocessing_File.Split_Metadata.in -metadata metadata4
mp straight-flow Flow_4 Read_and_Preprocessing_File.Split_Metadata.out Process_Enderecord.Format_Enderecord.in -metadata metadata4
mp straight-flow Process_Enderecord.Flow_2 Process_Enderecord.Format_Enderecord.out Process_Enderecord.Replicate_Enderecord.in -metadata metadata28
mp straight-flow Process_Enderecord.Flow_4 Process_Enderecord.Replicate_Enderecord.out Process_Enderecord.Reformat_Enderecord_for_Processing.in -metadata metadata28
mp straight-flow Flow_6 Process_Enderecord.Reformat_Enderecord_for_Processing.out.out0 Reformat_for_DB_and_Filter_out_where_Kompl_Kennzeichen_L_.in -metadata metadata28
mp straight-flow Flow_7 Reformat_for_DB_and_Filter_out_where_Kompl_Kennzeichen_L_.out.out0 Update_Insert_DWH_TA_K_RECH_ABSGRP.in -metadata metadata29 -buffer
mp straight-flow Process_Enderecord.Flow_3 Process_Enderecord.Replicate_Enderecord.out Process_Enderecord.Reformat_Enderecord_for_Update.in -metadata metadata28
mp straight-flow Process_Enderecord.Flow_1 Process_Enderecord.Reformat_Enderecord_for_Update.out.out0 Process_Enderecord.Update_DWH_TA_K_MELDUNGEN.in -metadata metadata30 -buffer
# mp straight-flow Splitting_and_Validating_the_reference_data.Flow_2 Splitting_and_Validating_the_reference_data.Reformat_fur_testzwecke.out.out0 Splitting_and_Validating_the_reference_data.Output_Table_TESTDATEN__table_.write
mp straight-flow Insert_Routine.Processing_with_sonstige_Positionen_.Flow_4 Insert_Routine.Processing_with_sonstige_Positionen_.Reformat_for_insert_fact_data_.out.out0 Insert_Routine.Processing_with_sonstige_Positionen_.DWH_TA_F_RPOS_CARM__table_.write -metadata metadata23
mp straight-flow Insert_Routine.Processing_with_sonstige_Positionen_.Flow_2 Insert_Routine.Processing_with_sonstige_Positionen_.Reformat_for_insert_temporary_data_.out.out0 Insert_Routine.Processing_with_sonstige_Positionen_.DWH_TA_T_RPOS_CARM__table_.write -metadata metadata24
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Flow_3 Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Reformat_for_insert_Factoring_Gutschriften_.out.out0 Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.DWH_TA_F_GPOS_FACT_CARM__table_.write -metadata metadata16
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Flow_2 Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Reformat_for_insert_Factoring_Rechnungen_.out.out0 Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.DWH_TA_F_RPOS_FACT_CARM__table_.write -metadata metadata15
mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Flow_7 Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.Reformat_for_insert_Reselling_.out.out0 Insert_Routine.Processing_without_sonstige_Positionen_.Router_rpos_geschaeftsform_kenn.DWH_TA_F_RPOS_RESELLING_CARM__table_.write -metadata metadata17
# mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Flow_3 Insert_Routine.Processing_without_sonstige_Positionen_.Sort.out Insert_Routine.Processing_without_sonstige_Positionen_.Dedup_Sorted.in
# mp straight-flow Insert_Routine.Processing_without_sonstige_Positionen_.Flow_4 Insert_Routine.Processing_without_sonstige_Positionen_.Dedup_Sorted.out Insert_Routine.Processing_without_sonstige_Positionen_._vertrags_id_rechnung_id_rechnung_datum_standardvertrags_id_rech_leistung_id_carm_dwh_vertrag_id_descending_prob_vertrag_id_monats_id_debitor_id_abs_grp_prob_provider_kenn_dwh_gp_id_dwh_konto_id_vo_kenn_rahmenvertrag_id_dwh_tarifgr_id_rpos_geschaftsform_kenn_vas_kenn_.in

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


# SCRIPT CONVERSION DESIGN DOCUMENT
**Target:** Legacy Ab Initio GDE Compiled KornShell Wrapper (`map_rpos_carmen_import.ksh`) to Modern Python 3

---

### 1. SCRIPT OVERVIEW
The legacy KornShell script `map_rpos_carmen_import.ksh` is an Ab Initio compiled wrapper script (generated by GDE version 1.14.16) that manages a high-volume ETL pipeline for importing Carmen RPOS billing/factoring transaction data. Sourced by UC4/Automic schedulers, the script reads retail billing flat files (defined by framework parameters), performs complex sorting, joins transactions with active contract history tables in Oracle, applies business transformations/validations/rollups, and securely updates multiple Target Data Warehouse (DWH) tables. The pipeline ensures transactional idempotency by querying and deleting existing target rows matching the current batch keys prior to executing final bulk inserts.

---

### 2. INVOCATION CONTEXT
*   **Invoker:** UC4/Automic Job Scheduler (via `JOBS_UNIX` object; specific name not supplied in extraction).
*   **Command Line / Arguments:** 
    *   Executed with optional positional arguments `$@`, which are passed downstream to project configuration hooks.
    *   Supports special argument `-reposit-tracking` to enable repository check-ins or version tracking.
    *   Supports `-help` which exits with status code `1`.
*   **UC4 Native Includes:** 
    *   None explicitly detected in source code.
*   **Environment Files Sourced:**
    *   `"${_AB_SAVED_PROJECT_DIR}"/.project.ksh` (called via dynamic wrapper `__AB_INVOKE_PROJECT` at the start and end of processing).  
        `# REVIEW-STRUCT: environment file [.project.ksh] not supplied — variables it sets are unknown; do not guess their names or values`
    *   `ab_catalog_functions.ksh` (sourced from `$AB_HOME/bin/` if present).  
        `# REVIEW-STRUCT: environment file [ab_catalog_functions.ksh] not supplied — variables it sets are unknown; do not guess their names or values`
    *   `./${_AB_PROXY_DIR}/GDE-Parameters` (locally written parameters).  
        `# REVIEW-STRUCT: environment file [GDE-Parameters] not supplied — variables it sets are unknown; do not guess their names or values`

---

### 3. PARAMETERS / INPUTS
The script declares environment variables to evaluate parameters. Unused parameters are flagged for structural awareness.

#### A. DB-Connection-Style Parameters (Cross-Referenced Convention)
`# REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying`

| Name | Source | Status in Body | Python Transition Mapping |
| :--- | :--- | :--- | :--- |
| `DB_TNS_NAME_DWH` | Env Var | **USED** | `os.environ.get("DB_TNS_NAME_DWH")` |
| `DB_USER_DWH` | Env Var | **USED** | `os.environ.get("DB_USER_DWH")` |
| `DB_PASSWD_DWH` | Env Var | **USED** | `os.environ.get("DB_PASSWD_DWH")` |
| `DB_TNS_NAME_CRS` | Env Var | Declared but unused | `os.environ.get("DB_TNS_NAME_CRS")` (flagged — verify) |
| `DB_USER_CRS` | Env Var | Declared but unused | `os.environ.get("DB_USER_CRS")` (flagged — verify) |
| `DB_PASSWD_CRS` | Env Var | Declared but unused | `os.environ.get("DB_PASSWD_CRS")` (flagged — verify) |
| `DB_TNS_NAME_SGM` | Env Var | Declared but unused | `os.environ.get("DB_TNS_NAME_SGM")` (flagged — verify) |
| `DB_USER_SGM` | Env Var | Declared but unused | `os.environ.get("DB_USER_SGM")` (flagged — verify) |
| `DB_PASSWD_SGM` | Env Var | Declared but unused | `os.environ.get("DB_PASSWD_SGM")` (flagged — verify) |
| `DB_TNS_NAME_CADS`| Env Var | Declared but unused | `os.environ.get("DB_TNS_NAME_CADS")` (flagged — verify) |
| `DB_USER_CADS` | Env Var | Declared but unused | `os.environ.get("DB_USER_CADS")` (flagged — verify) |
| `DB_PASSWD_CADS`| Env Var | Declared but unused | `os.environ.get("DB_PASSWD_CADS")` (flagged — verify) |
| `DB_TNS_NAME_CACM`| Env Var | Declared but unused | `os.environ.get("DB_TNS_NAME_CACM")` (flagged — verify) |
| `DB_USER_CACM` | Env Var | Declared but unused | `os.environ.get("DB_USER_CACM")` (flagged — verify) |
| `DB_PASSWD_CACM`| Env Var | Declared but unused | `os.environ.get("DB_PASSWD_CACM")` (flagged — verify) |

#### B. Framework & Job Configuration Parameters

| Name | Source | Status in Body | Python Transition Mapping |
| :--- | :--- | :--- | :--- |
| `AB_HOME` | Env Var | **USED** (Ab Initio Engine Path) | Internal logic replaces engine commands |
| `AB_JOB` | Env Var | **USED** (Graph execution prefix) | Maps to log configuration name |
| `BHB_Projektverzeichnis` | Env Var | Evaluated, unused | `os.environ.get("BHB_Projektverzeichnis")` |
| `BHB_Graph` | Env Var | Evaluated, unused | `os.environ.get("BHB_Graph")` |
| `BHB_Prozesstyp` | Env Var | Evaluated, unused | `os.environ.get("BHB_Prozesstyp")` |
| `BHB_Eintragsnr` | Env Var | **USED** (Metadata Entry ID) | Parse as integer: `int(os.environ["BHB_Eintragsnr"])` |
| `BHB_Quellverzeichnis` | Env Var | Evaluated, unused | `os.environ.get("BHB_Quellverzeichnis")` |
| `BHB_Zielverzeichnis` | Env Var | Evaluated, unused | `os.environ.get("BHB_Zielverzeichnis")` |
| `BHB_Dateimaske` | Env Var | Evaluated, unused | `os.environ.get("BHB_Dateimaske")` |
| `BHB_Kopfdatensatzkennung` | Env Var | Evaluated, unused | `os.environ.get("BHB_Kopfdatensatzkennung")` |
| `BHB_Nutzdatensatzkennung` | Env Var | **USED** (Identifies data rows) | `os.environ.get("BHB_Nutzdatensatzkennung")` |
| `BHB_Endedatensatzkennung` | Env Var | **USED** (Identifies EOF trailer) | `os.environ.get("BHB_Endedatensatzkennung")` |
| `BHB_Dateiname` | Env Var | **USED** (Incoming raw filename) | `os.environ.get("BHB_Dateiname")` |
| `BHB_DB` | Env Var | **USED** (Oracle config path) | Python Database Pool Configuration |
| `BHB_SAP_DML` | Env Var | **USED** (Metadata schema files) | Informational (re-implemented as schemas) |
| `BHB_DML` | Env Var | **USED** (Metadata schema files) | Informational (re-implemented as schemas) |

---

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
The compiled wrapper invokes Ab Initio Command Line interface utilities to orchestrate the pipeline. 

```bash
# Verbatim Commands Observed in Legacy Script:
uname
m_env -get AB_GRAPH_SCRIPT_REPOSIT_TRACKING
air sandbox find "${PROJECT_DIR}" -project
air rm -r -f "${AB_AIR_JOB}"
air mv "${AB_ORIGINAL_AIR_JOB}" "${AB_AIR_JOB}"
m_rmcatalog -catalog GDE-map_rpos_carmen_import-${AB_JOB}.cat
m_mkcatalog -catalog GDE-map_rpos_carmen_import-${AB_JOB}.cat
mp job ${AB_JOB}
m_db_layout layout3 ${BHB_DB}/DWH_BHB.dbc -serial
mp run
mp reset
m_rmcatalog
```

#### Transition Strategy:
*   These utility wrappers (e.g. `mp`, `air`) are **NOT** resolvable launchers because they represent an entire proprietary ETL engine orchestration rather than simple SQL script launchers.
*   **Design Decision:** The Python 3 target must **not** execute these external legacy binaries via `subprocess`. Instead, the file system operations, sorting, formatting transformations, and SQL lookups described inside the graph execution must be re-engineered natively in Python using the `pandas` data processing library and `oracledb` client engine.

---

### 5. EMBEDDED SQL
The script outputs SQL parameters and statements to a dynamic proxy workspace (`_AB_PROXY_DIR`) to run lookup, delete, and insert operations.

`# REVIEW: target database platform not specified; DB-client library choice (Oracle/oracledb) below is provisional based on TNS name parameters`

#### SQL Statement 1: Factoring Records Deletion (`Delete_rows_from_DWH_TA_F_RPOS_CARM-4.sql`)
*   **Table:** `DWH$TA_F_RPOS_CARM`
*   **Type:** `DELETE`
*   **Dialect:** Oracle SQL
*   **Statement Text:**
```sql
DELETE FROM DWH$TA_F_RPOS_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```

#### SQL Statement 2: GPOS Fact Records Deletion (`Delete_rows_from_DWH_TA_F_GPOS_FACT_CARM-60.sql`)
*   **Table:** `DWH$TA_F_GPOS_FACT_CARM`
*   **Type:** `DELETE`
*   **Dialect:** Oracle SQL
*   **Statement Text:**
```sql
DELETE FROM DWH$TA_F_GPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```

#### SQL Statement 3: RPOS Alternate Key Deletion (`Delete_rows_from_DWH_TA_F_RPOS_CARM_2-61.sql`)
*   **Table:** `DWH$TA_F_RPOS_CARM`
*   **Type:** `DELETE`
*   **Dialect:** Oracle SQL
*   **Statement Text:**
```sql
DELETE FROM DWH$TA_F_RPOS_CARM
WHERE  rechnung_datum = :rechnung_datum
AND    rechnung_id = :rechnung_id
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```

#### SQL Statement 4: Fact RPOS Records Deletion (`Delete_rows_from_DWH_TA_F_RPOS_FACT_CARM-62.sql`)
*   **Table:** `DWH$TA_F_RPOS_FACT_CARM`
*   **Type:** `DELETE`
*   **Dialect:** Oracle SQL
*   **Statement Text:**
```sql
DELETE FROM DWH$TA_F_RPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```

#### SQL Statement 5: Reselling RPOS Records Deletion (`Delete_rows_from_DWH_TA_F_RPOS_RESELLING_CARM-63.sql`)
*   **Table:** `DWH$TA_F_RPOS_RESELLING_CARM`
*   **Type:** `DELETE`
*   **Dialect:** Oracle SQL
*   **Statement Text:**
```sql
DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id
```

#### SQL Statement 6: Temporary RPOS Records Deletion (`Delete_rows_from_DWH_TA_T_RPOS_CARM-65.sql`)
*   **Table:** `DWH$TA_T_RPOS_CARM`
*   **Type:** `DELETE`
*   **Dialect:** Oracle SQL
*   **Statement Text:**
```sql
DELETE FROM DWH$TA_T_RPOS_CARM
WHERE  debitor_id = :debitor_id
AND    rechnung_datum = :rechnung_datum
AND    rechnung_id = :rechnung_id
```

#### SQL Statement 7: Metadata/Aggregate Table Update (`Update_Insert_DWH_TA_K_RECH_ABSGRP-70.sql`)
*   **Table:** `DWH$TA_K_RECH_ABSGRP`
*   **Type:** `UPDATE`
*   **Dialect:** Oracle SQL
*   **Statement Text:**
```sql
UPDATE DWH$TA_K_RECH_ABSGRP
SET   rechnung_datum = :rechnung_datum, 
      ladedatum = :ladedatum
WHERE  monats_id = :monats_id
AND    abs_grp = :abs_grp
AND    dateiname = :dateiname
AND    rechnungsteil = :rechnungsteil
```

#### SQL Statement 8: Metadata/Aggregate Table Insert (`Update_Insert_DWH_TA_K_RECH_ABSGRP-71.sql`)
*   **Table:** `DWH$TA_K_RECH_ABSGRP`
*   **Type:** `INSERT`
*   **Dialect:** Oracle SQL
*   **Statement Text:**
```sql
INSERT INTO DWH$TA_K_RECH_ABSGRP (monats_id, abs_grp, dateiname,  rechnung_datum, rechnungsteil, ladedatum)
VALUES (:monats_id, :abs_grp, :dateiname,  :rechnung_datum, :rechnungsteil, :ladedatum)
```

#### SQL Statement 9: Log/System Notification Update (`Update_DWH_TA_K_MELDUNGEN-74.sql`)
*   **Table:** `dwh$ta_k_meldungen`
*   **Type:** `UPDATE`
*   **Dialect:** Oracle SQL
*   **Statement Text:**
```sql
update dwh$ta_k_meldungen 
set anzahl_ds_eof = :anzahl
  , dateiname = :dateiname
  , enderecord_text = :inhalt
  , zusatzinfo = :bemerkung 
where entrynr = :eintragsnr
```

#### SQL Statement 10: Contract Lookup Query (Embedded in Lookups)
*   **Table:** `dwh$ta_c_vertrag`
*   **Type:** `SELECT`
*   **Dialect:** Oracle SQL (using native outer join `(+)` operator)
*   **Statement Text:**
```sql
select 
rahmenvertrag_id,
vertrag_id_carmen,
dwh_vertrag_id,
dwh_gp_id,
dwh_konto_id,
dwh_tarifgr_id,
vo_kenn,
zv_id,
gueltig_von, 
gueltig_bis
from 
dwh$ta_c_vertrag
where 
gueltig_bis >= to_date('20050401', 'YYYYMMDD')
```

---

### 6. CONTROL FLOW
The script processes transactions sequentially:

1.  **Environment Configuration:** Initialize the Ab Initio ecosystem settings, PATH structures, compatibility variables, and fallback execution behaviors.
2.  **CLI Argument Validation:** Evaluate argument flags. Exit immediately if `-help` is parsed.
3.  **Core Parameter Evaluation:** Check mandatory connection and framework environment variables (`DB_USER_DWH`, `BHB_Dateiname`, etc.). Raise immediate errors if evaluations yield return codes other than `0`.
4.  **System/Numeric Compliance Block:** Set NLS parameter `NLS_NUMERIC_CHARACTERS=". "` to normalize system float parsing.
5.  **Dynamic Workspace Preparation:** Construct proxy execution folder (`_AB_PROXY_DIR`). Setup active `trap` statements to remove directories on standard exit signals (`EXIT`, `HUP`, `INT`, `QUIT`, `TERM`).
6.  **Create Pipeline Specifications:** Dump the legacy metadata structures (`.dml`), mapping instructions (`.xfr`), and target operations (`.sql`) into the workspace.
7.  **Read and Filter Inbound File:**
    *   Stream the input file (`BHB_Dateiname`).
    *   Split lines based on row markers: Rows matching `BHB_Nutzdatensatzkennung` flow to data processing; rows matching `BHB_Endedatensatzkennung` flow to control status checks.
8.  **Data Processing & Validation Phase:**
    *   Clean numeric fields, enforce numeric format checks, and handle decimal conversions.
    *   Query contract definitions (`dwh$ta_c_vertrag`) active for the current batch.
    *   Left-join customer records with contract table definitions.
    *   Perform multi-criteria temporal validation (`gueltig_von` <= Batch Window < `gueltig_bis`).
    *   Calculate validity ranking indices to eliminate redundant contract profiles, selecting the most active segment (`rankindex == 1`).
9.  **Aggregation Rollup:**
    *   Calculate the sum of transaction metrics: `rechpos_brutto_eur`, `rechpos_netto_eur`, and `rechpos_mwst_eur` grouped by invoice dimensions.
10. **Target Idempotency Phase:**
    *   Identify unique invoices present in the incoming stream.
    *   Run dynamic queries on target DWH tables (`DWH$TA_F_RPOS_CARM`, `DWH$TA_T_RPOS_CARM`, `DWH$TA_F_GPOS_FACT_CARM`, `DWH$TA_F_RPOS_FACT_CARM`, `DWH$TA_F_RPOS_RESELLING_CARM`).
    *   Execute targeted bulk deletes on active matching records.
11. **Route and Bulk Load Phase:**
    *   Apply transaction-type routes (Factoring Bills, Factoring Credits, Reselling, temporary datasets).
    *   Execute parallel bulk-loading scripts into DWH targets.
12. **Audit & Log updates:**
    *   Parse the Trailer/Enderecord to verify integrity metrics (e.g. record counts).
    *   Update `DWH$TA_K_RECH_ABSGRP` with run dates and tracking data.
    *   Write statistics to system notifications registry `dwh$ta_k_meldungen`.
13. **Graceful Workspace Teardown:** Trigger proxy directory deletions, disconnect DB streams, and exit with status code `0`.

---

### 7. ERROR HANDLING & EXIT CODES
*   **Error Detection:**
    *   Monitors variable evaluations via checking `$mpjret` immediately after declarations.
    *   Validates DB interface operations; pipeline crashes instantly on connection failure or missing database objects.
    *   Validates files; crashes instantly on corrupted files or schema mismatches.
*   **Clean-up Mechanism:** Dynamic Shell traps (`trap '__AB_CLEANUP_PROXY_FILES' EXIT`) are mapped to clear physical assets inside the runtime workspace if a signal interruption terminates execution.
*   **Python Conversion Strategy:** 
    *   Process validations natively via raising `ValueError` or standard Python assert mechanisms.
    *   Wrap SQL transactions inside standard `try ... except ... finally` blocks to ensure automatic rollback on failure and release connection handles.
    *   Replicate clean-up operations using Python's standard `atexit` registry or `finally` context managers to delete temporary system variables or tracking files.

---

### 8. OUTPUTS / SIDE EFFECTS
*   **Database Target Transactions:**
    *   **Delete/Insert:** `DWH$TA_F_RPOS_CARM`
    *   **Delete/Insert:** `DWH$TA_T_RPOS_CARM`
    *   **Delete/Insert:** `DWH$TA_F_GPOS_FACT_CARM`
    *   **Delete/Insert:** `DWH$TA_F_RPOS_FACT_CARM`
    *   **Delete/Insert:** `DWH$TA_F_RPOS_RESELLING_CARM`
    *   **Merge/Update:** `DWH$TA_K_RECH_ABSGRP`
    *   **Update:** `dwh$ta_k_meldungen`
*   **File Changes:**
    *   Reads and processes the incoming retail file identified in `${BHB_Dateiname}`.

---

### 9. BUSINESS SUMMARY
*   **Retail Billing Engine:** Coordinates the main data load of Carmen RPOS transactions into Core Finance platforms.
*   **Data Validation:** Cleans input records, normalizes floating-point characters, and flags format discrepancies.
*   **Active Customer Verification:** Leverages active contract dates (`dwh$ta_c_vertrag`) to map every incoming retail invoice to a valid customer contract ID.
*   **High-Volume Processing:** Groups complex billing items (Factoring invoices, Credit records, Reselling data) and streams sorted batches to target relational engines.
*   **Job Idempotency:** Safely clears historical loads of matching transaction periods prior to processing to prevent duplicated values.
*   **Audit Compliance Tracking:** Parses physical file trailer metadata to perform data verification, logging statistical run checks directly into system metrics tables.

---

### 10. PYTHON PSEUDOCODE OUTLINE

```python
#!/usr/bin/env python3
"""
Python 3 implementation of the legacy 'map_rpos_carmen_import.ksh' Ab Initio ETL wrapper.
Leverages pandas for schema transformations and oracledb for transactional database operations.
"""

import os
import sys
import argparse
import logging
import datetime
import shutil
import tempfile
import oracledb
import pandas as pd

# Set up logging format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("map_rpos_carmen_import")

# Step 1: Parse arguments and environment settings
def parse_arguments():
    parser = argparse.ArgumentParser(description="Carmen RPOS Billing Import pipeline")
    parser.add_argument('-reposit-tracking', action='store_true', help="Enable repository tracking simulation")
    # Capturing remaining arbitrary args passed down from UC4
    parser.add_argument('args', nargs=argparse.REMAINDER)
    return parser.parse_args()

def validate_environment():
    # Enforce mandatory variable validation
    required_vars = [
        "DB_TNS_NAME_DWH", "DB_USER_DWH", "DB_PASSWD_DWH",
        "BHB_Dateiname", "BHB_Nutzdatensatzkennung", "BHB_Endedatensatzkennung", "BHB_Eintragsnr"
    ]
    env_params = {}
    for var in required_vars:
        val = os.environ.get(var)
        if not val:
            logger.error(f"Mandatory environment variable not set: {var}")
            sys.exit(1)
        env_params[var] = val
    return env_params

# Step 2: Establish DB Session Pool
def get_db_connection(params):
    try:
        # Connect to Oracle database using the thin driver
        dsn = params["DB_TNS_NAME_DWH"]
        user = params["DB_USER_DWH"]
        password = params["DB_PASSWD_DWH"]
        connection = oracledb.connect(user=user, password=password, dsn=dsn)
        return connection
    except Exception as e:
        logger.error(f"Database connection failure using DSN {dsn}: {e}")
        sys.exit(1)

# Step 3: Stream and split incoming files
def parse_input_file(filepath, nutz_id, end_id):
    logger.info(f"Streaming data from file source: {filepath}")
    data_rows = []
    trailer_rows = []
    
    if not os.path.exists(filepath):
        logger.error(f"Input file path not found: {filepath}")
        sys.exit(1)

    with open(filepath, 'r', encoding='latin-1') as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            # Split records on semicolon or structural layout logic
            first_char = stripped[0]
            if first_char == nutz_id:
                data_rows.append(stripped)
            elif first_char == end_id:
                trailer_rows.append(stripped)
                
    return data_rows, trailer_rows

# Step 4: Normalization and validation operations
def transform_nutzdaten(raw_rows):
    logger.info(f"Transforming {len(raw_rows)} transaction lines")
    # Emulate the 'Reformat_for_DB-20.xfr' and 'Validate_Records' logic
    # Set numeric character parsing conventions locally
    parsed_records = []
    for row in raw_rows:
        fields = row.split(';') # Map delimiter logic
        try:
            # Map parameters based on legacy metadata specs
            monats_id = fields[1][:6] # YYYYMM format
            rechnung_id = fields[3].strip()
            rechnung_datum = datetime.datetime.strptime(fields[4], "%Y%m%d").date()
            
            # Numeric values clean decimals and parse formats
            standardvertrags_id = int(fields[5]) if fields[5] != "#" else 0
            vertrags_id = int(fields[6]) if fields[6] != "#" else 0
            
            # Map finance properties: emulate NLS_NUMERIC_CHARACTERS = ". "
            brutto = float(fields[8].replace(',', '.'))
            netto = float(fields[9].replace(',', '.'))
            mwst = float(fields[10].replace(',', '.'))
            
            parsed_records.append({
                "monats_id": monats_id,
                "debitor_id": fields[2].strip(),
                "rechnung_id": rechnung_id,
                "rechnung_datum": rechnung_datum,
                "standardvertrags_id": standardvertrags_id,
                "vertrags_id": vertrags_id,
                "rech_leistung_id_carm": fields[7].strip(),
                "rechpos_brutto_eur": brutto,
                "rechpos_netto_eur": netto,
                "rechpos_mwst_eur": mwst,
                "pooling": fields[11].strip() if len(fields) > 11 else "#",
                "rpos_geschaftsform_kenn": fields[12].strip() if len(fields) > 12 else "#"
            })
        except Exception as e:
            logger.error(f"Row parsing or data compliance violation: {e} | Row: {row}")
            raise ValueError(f"Invalid record structure format: {e}")
            
    return pd.DataFrame(parsed_records)

# Step 5: Dynamic SQL outer join logic with Contract registry table
def fetch_contract_history(conn):
    logger.info("Fetching active contract registry from dwh$ta_c_vertrag")
    # Matches SQL: 'select ... from dwh$ta_c_vertrag where gueltig_bis >= to_date('20050401', 'YYYYMMDD')'
    query = """
        SELECT 
            vertrag_id_carmen AS vertrags_id,
            rahmenvertrag_id,
            dwh_vertrag_id,
            dwh_gp_id,
            dwh_konto_id,
            dwh_tarifgr_id,
            vo_kenn,
            zv_id,
            gueltig_von,
            gueltig_bis
        FROM dwh$ta_c_vertrag
        WHERE gueltig_bis >= TO_DATE('20050401', 'YYYYMMDD')
    """
    return pd.read_sql(query, con=conn)

def join_and_rank_records(df_transactions, df_contracts):
    logger.info("Executing left join and calculating contract rankings")
    # Join on vertrags_id
    merged = pd.merge(df_transactions, df_contracts, on="vertrags_id", how="left")
    
    # Evaluate Temporal validity dates constraints
    # filter entries: (gueltig_von <= month_last_day) and (gueltig_bis is null or month_last_day <= gueltig_bis)
    # Calculate YYYYMM month end datetime objects
    def get_month_end(monats_str):
        year = int(monats_str[:4])
        month = int(monats_str[4:6])
        if month == 12:
            return datetime.date(year, 12, 31)
        return datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
        
    merged['month_last_day'] = merged['monats_id'].apply(get_month_end)
    
    # Filter rows based on temporal validation logic
    valid_contracts = merged[
        (merged['gueltig_von'].isna() | (merged['month_last_day'] > merged['gueltig_von'].dt.date)) &
        (merged['gueltig_bis'].isna() | (merged['month_last_day'] <= merged['gueltig_bis'].dt.date))
    ].copy()
    
    # Replicate Scan Ranking block
    # Sort: [vertrags_id, rechnung_id, rechnung_datum, standardvertrags_id] and temporal descending
    valid_contracts.sort_values(
        by=["vertrags_id", "rechnung_id", "rechnung_datum", "standardvertrags_id", "gueltig_von", "dwh_vertrag_id"],
        ascending=[True, True, True, True, False, False],
        inplace=True
    )
    
    # Retain the top rank record for each primary key (rankindex == 1)
    pk_cols = ["vertrags_id", "rechnung_id", "rechnung_datum", "standardvertrags_id", "rech_leistung_id_carm"]
    ranked_records = valid_contracts.groupby(pk_cols).first().reset_index()
    
    return ranked_records

# Step 6: Target Idempotency Deletions
def execute_target_deletions(conn, df_targets):
    logger.info("Ensuring idempotency: executing targeted DWH records deletion")
    cursor = conn.cursor()
    
    # Extract unique key parameters to execute single-batch clear operations
    delete_keys = df_targets[["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id"]].drop_duplicates().values.tolist()
    
    delete_queries = [
        # DWH$TA_F_RPOS_CARM Deletion
        "DELETE FROM DWH$TA_F_RPOS_CARM WHERE rechnung_id = :1 AND rechnung_datum = :2 AND standardvertrags_id = :3 AND vertrags_id = :4",
        # DWH$TA_F_GPOS_FACT_CARM Deletion
        "DELETE FROM DWH$TA_F_GPOS_FACT_CARM WHERE rechnung_id = :1 AND rechnung_datum = :2 AND standardvertrags_id = :3 AND vertrags_id = :4",
        # DWH$TA_F_RPOS_FACT_CARM Deletion
        "DELETE FROM DWH$TA_F_RPOS_FACT_CARM WHERE rechnung_id = :1 AND rechnung_datum = :2 AND standardvertrags_id = :3 AND vertrags_id = :4",
        # DWH$TA_F_RPOS_RESELLING_CARM Deletion
        "DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM WHERE rechnung_id = :1 AND rechnung_datum = :2 AND standardvertrags_id = :3 AND vertrags_id = :4"
    ]
    
    try:
        for sql in delete_queries:
            cursor.executemany(sql, delete_keys)
        conn.commit()
        logger.info(f"Target clear transactions completed successfully across {len(delete_keys)} key parameters.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Idempotent deletion failure: {e}")
        raise e
    finally:
        cursor.close()

# Step 7: Final target bulk loads
def bulk_insert_targets(conn, df_ranked):
    logger.info("Starting target table data streaming")
    # Perform aggregation grouping rollups mimicking phase 1 targets
    # Router logic matches 'rpos_geschaftsform_kenn' and splits to Factoring / Reselling / Temp targets
    # Insert code block omitted for brevity - uses cursor.executemany() for fast bulk batch inserts
    pass

# Step 8: System audit logging
def process_enderecord_audits(conn, trailer_rows, file_name, entry_nr):
    logger.info("Processing trailer statistics logging")
    if not trailer_rows:
        logger.warning("No Enderecord found in current run source file")
        return
        
    fields = trailer_rows[0].split(';')
    # Emulate 'Reformat_Enderecord_for_Update' logic
    anzahl_ds = int(fields[3]) if fields[3].isdigit() else 0
    inhalt = fields[4]
    bemerkung = fields[1]
    
    cursor = conn.cursor()
    try:
        # Step 8A: Update dwh$ta_k_meldungen
        meldungen_sql = """
            UPDATE dwh$ta_k_meldungen 
            SET anzahl_ds_eof = :1
              , dateiname = :2
              , enderecord_text = :3
              , zusatzinfo = :4 
            WHERE entrynr = :5
        """
        cursor.execute(meldungen_sql, (anzahl_ds, file_name, inhalt, bemerkung, entry_nr))
        conn.commit()
        logger.info("Trailer logging stats successfully registered in system registries")
    except Exception as e:
        conn.rollback()
        logger.error(f"Audit log writing failure: {e}")
    finally:
        cursor.close()

# Step 9: Control Coordinator execution block
def main():
    args = parse_arguments()
    env_params = validate_environment()
    
    # Establish Oracle database connection
    db_conn = get_db_connection(env_params)
    
    try:
        # Read file
        raw_data, raw_trailer = parse_input_file(
            env_params["BHB_Dateiname"], 
            env_params["BHB_Nutzdatensatzkennung"], 
            env_params["BHB_Endedatensatzkennung"]
        )
        
        # Enforce validation and structure mappings
        df_transactions = transform_nutzdaten(raw_data)
        
        # Pull reference contract metadata
        df_contracts = fetch_contract_history(db_conn)
        
        # Merge references and compute rankings
        df_ranked = join_and_rank_records(df_transactions, df_contracts)
        
        # Clear duplicate key spaces in targets (Ensure idempotency)
        execute_target_deletions(db_conn, df_ranked)
        
        # Run final parallel target updates
        bulk_insert_targets(db_conn, df_ranked)
        
        # Perform EOF audit operations
        process_enderecord_audits(db_conn, raw_trailer, env_params["BHB_Dateiname"], int(env_params["BHB_Eintragsnr"]))
        
        logger.info("Carmen Import pipeline executed successfully.")
        sys.exit(0)
        
    except Exception as e:
        logger.critical(f"Carmen Import run failed due to processing exception: {e}")
        sys.exit(1)
    finally:
        if db_conn:
            db_conn.close()

if __name__ == "__main__":
    main()
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh` | `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py` | Converts the legacy Ab Initio KornShell wrapper to a Python script that orchestrates the execution parameters, executes preliminary metadata validations, manages the target BigQuery idempotent deletes, and triggers the PySpark pipeline representing the migrated Ab Initio graph. |

---

### Job Dependencies
*   **Upstream Dependencies:**
    *   **Shared Files / Common Utilities:** Sourced from `abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start`. This utility module has already been migrated and merged (PR: https://github.com/gurunathan-prodapt/pi-agents/pull/767). In the target BigQuery environment, the python wrapper `map_rpos_carmen_import.py` will import the converted `r_ai_start` Python module to leverage common initialization hooks and standard logging setups instead of recreating them.

---

### Execution Order
The Cloud Composer DAG orchestration must preserve the legacy execution order:
1.  **Orchestration Metadata Trigger:** `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml` (the UC4 orchestration layer initiates the sequence).
2.  **Configuration Parameter Resolution:** Slices settings from the converted config file `abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.cfg` to parameterize runtime values.
3.  **Wrapper Orchestrator Python Execution:** Executes `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py` to prepare execution buffers, run checks, and execute transactional deletions on BigQuery.
4.  **PySpark Execution:** Triggers the PySpark pipeline converted from the Ab Initio graph `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp`.

---

### Lineage
*   **Legacy Outbound Linkage:**
    *   `map_rpos_carmen_import.ksh` --[INVOKES]--> `AB_CATALOG_FUNCTIONS.KSH`. This has been human-reviewed and confirmed as **NO SOURCE NEEDED** (retired; not required for python logic).
    *   `map_rpos_carmen_import.ksh` --[USES_GRAPH]--> `map_rpos_carmen_import.mp` (representing the underlying data-transformation graph). This is mapped to a PySpark job run on Dataproc Serverless, triggered and managed by our wrapper python script.

---

### External System Replacements
*   **Relational Database Engine:** Legacy Oracle Database is replaced by **Google BigQuery**.
    *   `DWH$TA_F_RPOS_CARM` $\rightarrow$ `BQ_DATASET.DWH_TA_F_RPOS_CARM`
    *   `DWH$TA_T_RPOS_CARM` $\rightarrow$ `BQ_DATASET.DWH_TA_T_RPOS_CARM`
    *   `DWH$TA_F_GPOS_FACT_CARM` $\rightarrow$ `BQ_DATASET.DWH_TA_F_GPOS_FACT_CARM`
    *   `DWH$TA_F_RPOS_FACT_CARM` $\rightarrow$ `BQ_DATASET.DWH_TA_F_RPOS_FACT_CARM`
    *   `DWH$TA_F_RPOS_RESELLING_CARM` $\rightarrow$ `BQ_DATASET.DWH_TA_F_RPOS_RESELLING_CARM`
    *   `DWH$TA_K_RECH_ABSGRP` $\rightarrow$ `BQ_DATASET.DWH_TA_K_RECH_ABSGRP`
    *   `dwh$ta_k_meldungen` $\rightarrow$ `BQ_DATASET.DWH_TA_K_MELDUNGEN`
    *   `dwh$ta_c_vertrag` $\rightarrow$ `BQ_DATASET.DWH_TA_C_VERTRAG`
*   **File Storage Mounts:** Legacy UNIX paths `$DW_DIR_IMP_SAP/crs/work/` and `$DW_DIR_IMP_SAP/crs/store/` are replaced by dedicated Cloud Storage URI endpoints (e.g., `gs://{GCS_BUCKET}/crs/work/` and `gs://{GCS_BUCKET}/crs/store/`).

---

### Cross-File Dependencies
*   **Ab Initio Graph Relationship:** The Python orchestrator `map_rpos_carmen_import.py` is dynamically linked to the migrated PySpark equivalent of `map_rpos_carmen_import.mp`. It relies on this script to handle the bulk transaction transformations after the idempotent deletion phases execute successfully.
*   **Configuration Dependency:** The wrapper requires values parsed from the configuration module `map_rpos_carmen_import.cfg` to resolve dataset masks and header markers at run time.

---

### Target File Plan

*   **Target File Path:** `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py`
    *   **Language:** Python 3
    *   **Source File:** `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh`
    *   **Purpose:** Replaces the legacy compiled KornShell wrapper. Reads framework configuration properties, processes raw file segments from Google Cloud Storage, performs temporal key validations, coordinates the execution of pre-insert BigQuery `DELETE` scripts, and submits the bulk-data-loading PySpark script to Dataproc Serverless.

---

### Environment-Specific Values

#### 1. GLOBAL (Environment-Wide Infrastructure)
*   **`GCP_PROJECT`**: Identifies the Google Cloud environment target project. 
    *   *Sourced at runtime via:* `Variable.get("GCP_PROJECT")` inside Cloud Composer DAGs.
*   **`GCS_BUCKET`**: Target cloud storage bucket hosting raw retail flat files.
    *   *Sourced at runtime via:* `Variable.get("GCS_BUCKET")` inside Cloud Composer DAGs.
*   **`BQ_DATASET`**: Identifies the destination BigQuery schema housing billing tables.
    *   *Sourced at runtime via:* `Variable.get("BQ_DATASET")` inside Cloud Composer DAGs.
*   **`GCP_REGION`**: Region executing Dataproc Serverless and Composer resources.
    *   *Sourced at runtime via:* `Variable.get("GCP_REGION")` inside Cloud Composer DAGs.
*   **`DW_DIR_IMP_SAP`**: Maps the historical UNIX file base directory.
    *   *Sourced at runtime via:* Resolved to a structured sub-folder within `GCS_BUCKET`.

#### 2. JOB-SPECIFIC
*   **`BHB_Dateiname`**: Dynamic incoming flat-file name containing billing transactions.
    *   *Sourced at runtime via:* Sourced dynamically from Airflow DAG trigger parameters (`dag_run.conf`).
*   **`BHB_Eintragsnr`**: Logging entry tracker ID mapped to audits.
    *   *Sourced at runtime via:* Passed as an execution run-id or task instance parameter in Cloud Composer.
*   **`BHB_Nutzdatensatzkennung`**: Demarcation string (e.g. `'P'`) for data rows.
    *   *Sourced at runtime via:* Set within the local job configuration parameters.
*   **`BHB_Endedatensatzkennung`**: Demarcation string (e.g. `'X'`) for file trailers.
    *   *Sourced at runtime via:* Set within the local job configuration parameters.

---

### Risks and Manual Steps
*   **Dependency on Graph Migration:** The Python wrapper expects the PySpark equivalent of `map_rpos_carmen_import.mp` to be successfully deployed on GCS. Orchestrated integration testing of this script cannot occur until the graph conversion has been finalized.
*   **Oracle outer join (+) syntax:** The contract history query in the legacy logic uses the Oracle outer join operator `c.vertrag_id_carmen (+) = :vertrags_id`. In BigQuery, this must be rewritten to standard `LEFT OUTER JOIN` syntax.
*   **BigQuery DML Efficiency (Idempotent Deletes):** Legacy logic executes targeted row deletions across five tables based on unique invoice-batch keys. In BigQuery, high-frequency single-record deletes are highly inefficient and subject to rate limits. It is recommended to perform deletes as a single merge metadata task or batch partition swap instead of processing individual key lookups.
*   **System Event and Statistics Logging:** The legacy workflow logs audit tracking indicators to Oracle tables `dwh$ta_k_meldungen` and `DWH$TA_K_RECH_ABSGRP`. A manual step is required to integrate this audit logging with the target enterprise's standard GCP monitoring/logging frameworks or a dedicated central BigQuery logging schema.