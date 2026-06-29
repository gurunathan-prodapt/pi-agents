# Migration Design — ausd_bp_ta_bpr_basis_his

## 1. Purpose & Scope
The job `ausd_bp_ta_bpr_basis_his` is a key snapshot-based historical extraction job within the **BERT system**. Its business purpose is to prepare and stage active and reactivatable base product instances (such as MultiSIM, Twin-Cards, voice products, and connections) linked to contracts, including formatting and staging physical attributes like SIM ICCID and IMSI fields.

The scope of this migration involves transitioning the orchestration, parameter control, and database extraction of base product history from a legacy legacy stack (UC4 scheduler, KornShell wrapper/control scripts, Oracle Database with remote DB-Link links) to the modern **Google Cloud Platform (GCP)**.
- **Legacy Orchestrator**: UC4 (Automic) UNIX Job definition
- **Legacy Scripting**: KornShell wrapper and validation scripts (`r_ausd_bp_ta_bpr_basis_his.ksh`, `k_ausd_bp_ta_bpr_basis_his.ksh`)
- **Legacy Processing Engine**: Oracle SQL*Plus script executing remote joins via `@pcrs1` database link.
- **Target Platform**: Google Cloud Composer (Apache Airflow) as orchestrator, executing Google BigQuery Standard SQL scripts for processing.

---

## 2. Source Inventory
The legacy job consists of 4 primary source files, summarized below:

| File Name / Key | Source Technology | Complexity Tier | Automation Bucket | Summary / Role |
| :--- | :--- | :--- | :--- | :--- |
| `DW.BERT_AUSD_BP_TA_BPR_BASIS_HIS.xml` | UC4/Automic Job XML | Medium | Semi-auto | Entry point UC4 Job that initializes global parameters (`AUSD_BP_TA_BASIS_HIS`) and invokes the wrapper script. |
| `r_ausd_bp_ta_bpr_basis_his.ksh` | KornShell | Medium | Semi-auto | Wrapper shell script executing environment checks, handling logging and runtime error trapping via custom modules (`f_alis_msgerr.ksh`). |
| `k_ausd_bp_ta_bpr_basis_his.ksh` | KornShell | Medium | Semi-auto | Control script that parses inputs, performs date check validations (`DWDate_Datum_Check`), and starts SQL execution. |
| `d_ausd_bp_ta_bpr_basis_his.sql` | Oracle SQL*Plus | Medium | Semi-auto | Core SQL script running table truncates and dynamic SELECT insertions by joining tables from the remote database link (`&v_carmen` / `@pcrs1`). |

---

## 3. Target Architecture
The proposed architecture on **Google Cloud Platform (GCP)** eliminates shell wrappers, OS-level cron dependencies, and complex SQL*Plus wrappers.

```
       +------------------------------------+
       |   Apache Airflow (Cloud Composer)  |
       +-----------------+------------------+
                         |
                         | triggers BigQuery Script
                         v
       +-----------------+------------------+
       |           Google BigQuery          |
       |  - Decodes metadata date context   |
       |  - Performs Truncate               |
       |  - Performs High-Performance Joins  |
       +------------------------------------+
```

### Components and Layout:
1. **Orchestrator**: Cloud Composer (Apache Airflow 2.x) runs a Python-based DAG (`ausd_bp_ta_bpr_basis_his`) that replaces the UC4 scheduler, `r_ausd_bp_ta_bpr_basis_his.ksh`, and `k_ausd_bp_ta_bpr_basis_his.ksh`.
2. **BigQuery ETL Scripting**: The dynamic SQL code from `d_ausd_bp_ta_bpr_basis_his.sql` is migrated into a BigQuery **Standard SQL scripting block** containing variable declaration (`DECLARE`), metadata queries (`SET`), standard truncation (`TRUNCATE TABLE`), and INSERT transformations.
3. **Table Relocations**:
   - The remote Oracle schema accessible via link `&v_carmen` (e.g. `cds$ta_cntrct` and `pds$ta_bpri_com`) is replaced with native BigQuery tables in target staging/source datasets (e.g., `project.cds.ta_cntrct` and `project.pds.ta_bpri_com`), which are replicated from the source systems via cloud ELT pipelines.
   - The target staging table `sof$ta_bpr_basis_his` becomes a native BigQuery table `project.sof.ta_bpr_basis_his`.

---

## 4. Data Flow & Lineage

The data flow runs as an idempotent batch job. The visual representation below illustrates the progression of data:

```
[project.isbert_schema.dwtk_meldungen] (Metadata Date Lookup)
                │
                ▼ (resolves v_datum)
[project.cds.ta_cntrct] ───────┐
                               ├─► [BigQuery JOIN & Transform] ─► [project.sof.ta_bpr_basis_his]
[project.pds.ta_bpri_com] ─────┘
```

### Step-by-Step Execution Flow:
1. **Airflow DAG Activation**: Triggered manually or by upstream scheduled dependency tasks.
2. **Retrieve Business Date (`v_datum`)**: Executes a BQ query against metadata table `project.isbert_schema.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'` to retrieve the target processing date context. If empty, defaults to `1900-01-01`.
3. **Truncate Target**: Clears the historical table `project.sof.ta_bpr_basis_his` to ensure safe reruns.
4. **Data Join and Transformation**: Queries replicated tables `cds.ta_cntrct` and `pds.ta_bpri_com`, applies filtering and concatenations, and inserts rows into `sof.ta_bpr_basis_his`.

---

## 5. Transformation Logic

### Key Legacy-to-Cloud Mappings:
1. **Remote Database Links**: The Oracle database link (`&v_carmen` / `@pcrs1`) is completely retired. Source tables are now referenced locally within BigQuery.
2. **Dynamic Date Query**:
   * *Legacy Oracle*:
     ```sql
     SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
       FROM isbert_schema.dwtk_meldungen m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
     ```
   * *BigQuery*:
     ```sql
     DECLARE v_datum DATE;
     SET v_datum = (
       SELECT COALESCE(DATE(MAX(timecreated)), DATE('1900-01-01'))
       FROM `isbert_schema.dwtk_meldungen`
       WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
     );
     ```
3. **ICCID String Concatenation**:
   In BigQuery, if any argument in `CONCAT` is NULL, the overall result becomes NULL. We use `COALESCE` on each individual sub-component of the legacy Oracle construct `bp.iccid_mi||'-'||bp.iccid_ii...` to guarantee empty-string fallback compatibility:
   * *BigQuery equivalent*:
     ```sql
     CONCAT(
       COALESCE(bp.iccid_mi, ''), '-',
       COALESCE(bp.iccid_ii, ''), '-',
       COALESCE(bp.iccid_iai, ''), '-',
       COALESCE(bp.iccid_nr, ''), '-',
       COALESCE(bp.iccid_cd, '')
     ) AS ICCID
     ```
4. **Filter Rules & Status IDs**:
   - Matches active and reactivatable contract statuses: `c.cntrct_st IN (5, 6)`
   - Keeps non-service provider owned agreements: `c.redundant_owner_id = 1`
   - Filters on target base product IDs (`bpr_id`):
     - `31` : tnv
     - `2759` : twin-card
     - `2800` : twin-bill-privat
     - `2835` : da-anschluss
     - `2836` : vda-anschluss
     - `2837` : tk-anschluss
     - `3848` : MultiSIM
5. **Optimizer Hints**: Oracle specific optimization hints (such as `/*+ DRIVING_SITE(c) ... */`) are fully removed, allowing BigQuery's query planner to optimize execution natively.

---

## 6. External Dependencies
- **Upstream Data Replication**: Sourced tables `cds.ta_cntrct` and `pds.ta_bpri_com` must be updated in BigQuery before the job is triggered. Cloud-native orchestrators should implement sensors checking for the arrival of new daily data partitions or update indicators.
- **Log Management System**: The custom shell library functions (`f_alis_msgerr.ksh` / `DWMSG_*`) are retired. All run-time and diagnostic logging is captured and monitored natively within **Google Cloud Logging** through Airflow's environment output.

---

## 7. Unresolved / Risks
- **Upstream Replication Latency**: A pipeline gap between operational source CRM/network systems and BigQuery can result in stale contract snapshots. 
- **Restart Param (`p_wiederanlaufWert`)**: Legacy shell parameters defined a restart checkpoint value (`wiederanlaufWert` / `-l`). However, the legacy database code (`d_ausd_bp_ta_bpr_basis_his.sql`) never utilized this variable, performing complete table truncation and reload. Following this design, the BigQuery script implements a native idempotent drop/repopulate strategy, removing unused restart configurations safely.
- **Null Values in ICCID Concatenations**: If any component (e.g., `iccid_mi`) is NULL in BQ, the `COALESCE(col, '')` prevents string truncation or failure.

---

## 8. Build Plan

### Component 1: Target Table BigQuery DDL
Creates the final analytical staged base product history table in BigQuery.

```sql
CREATE TABLE IF NOT EXISTS `sof.ta_bpr_basis_his` (
  CNTRCT_ID STRING OPTIONS(description="Contract identifier"),
  BPR_ID INT64 OPTIONS(description="Base product type identifier"),
  BPRI_COM_ID STRING OPTIONS(description="Product instance identifier"),
  ICCID STRING OPTIONS(description="SIM card serial number formatted"),
  IMSI_MCC STRING OPTIONS(description="Mobile Country Code"),
  IMSI_MNC STRING OPTIONS(description="Mobile Network Code"),
  IMSI_HLR STRING OPTIONS(description="Home Location Register indicator"),
  IMSI_SI STRING OPTIONS(description="Service Indicator"),
  CNTRCT_ID_REF STRING OPTIONS(description="Referenced contract identifier"),
  VALID_FROM DATE OPTIONS(description="Validity start date"),
  VALID_TO DATE OPTIONS(description="Validity end date"),
  MODIFIED_AT TIMESTAMP OPTIONS(description="Last modification timestamp"),
  INSERT_AT TIMESTAMP OPTIONS(description="Insertion timestamp"),
  SLAVE_NUMBER INT64 OPTIONS(description="MultiSIM sequence identifier"),
  E_ID STRING OPTIONS(description="SIM evolution e-ID")
)
PARTITION BY VALID_FROM;
```

### Component 2: BigQuery SQL Script (`d_ausd_bp_ta_bpr_basis_his.sql`)
Main data load logic, capturing date parameters dynamically and performing full load.

```sql
-- -----------------------------------------------------------------------------
-- BigQuery Script: d_ausd_bp_ta_bpr_basis_his.sql
-- Description: Dynamic processing of contract base product histories
-- -----------------------------------------------------------------------------

DECLARE v_datum DATE;

-- Retrieve dynamic processing timestamp context from the operational metadata
SET v_datum = (
  SELECT COALESCE(DATE(MAX(timecreated)), DATE('1900-01-01'))
  FROM `isbert_schema.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Safely clear destination table
TRUNCATE TABLE `sof.ta_bpr_basis_his`;

-- Insert targeted base product histories matching criteria
INSERT INTO `sof.ta_bpr_basis_his` (
  CNTRCT_ID,
  BPR_ID,
  BPRI_COM_ID,
  ICCID,
  IMSI_MCC,
  IMSI_MNC,
  IMSI_HLR,
  IMSI_SI,
  CNTRCT_ID_REF,
  VALID_FROM,
  VALID_TO,
  MODIFIED_AT,
  INSERT_AT,
  SLAVE_NUMBER,
  E_ID
)
SELECT
  bp.cntrct_id AS CNTRCT_ID,
  bp.bpr_id AS BPR_ID,
  bp.bpri_com_id AS BPRI_COM_ID,
  CONCAT(
    COALESCE(bp.iccid_mi, ''), '-',
    COALESCE(bp.iccid_ii, ''), '-',
    COALESCE(bp.iccid_iai, ''), '-',
    COALESCE(bp.iccid_nr, ''), '-',
    COALESCE(bp.iccid_cd, '')
  ) AS ICCID,
  bp.imsi_mcc AS IMSI_MCC,
  bp.imsi_mnc AS IMSI_MNC,
  bp.imsi_hlr AS IMSI_HLR,
  bp.imsi_si AS IMSI_SI,
  bp.cntrct_id_ref AS CNTRCT_ID_REF,
  bp.valid_from AS VALID_FROM,
  bp.valid_to AS VALID_TO,
  bp.modified_at AS MODIFIED_AT,
  bp.insert_at AS INSERT_AT,
  bp.slave_number AS SLAVE_NUMBER,
  bp.eid AS E_ID
FROM `cds.ta_cntrct` c
JOIN `pds.ta_bpri_com` bp
  ON c.cntrct_id = bp.cntrct_id
WHERE c.cntrct_st IN (5, 6)
  AND c.redundant_owner_id = 1
  AND DATE(c.insert_at) <= v_datum
  AND (c.modified_at IS NULL OR DATE(c.modified_at) > v_datum)
  AND DATE(c.valid_from) <= v_datum
  AND (c.valid_to IS NULL OR DATE(c.valid_to) > v_datum)
  AND c.is_production = 1
  AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
  -- Filter on target base product configuration IDs
  AND bp.bpr_id IN (
    31,   -- tnv
    2759, -- twin-card
    2800, -- twin-bill-privat
    2835, -- da-anschluss
    2836, -- vda-anschluss
    2837, -- tk-anschluss
    3848  -- MultiSIM
  )
  AND DATE(bp.insert_at) <= v_datum
  AND (bp.modified_at IS NULL OR DATE(bp.modified_at) > v_datum)
  AND DATE(bp.valid_from) <= v_datum
  AND bp.is_production = 1;
```

### Component 3: Airflow Orchestration DAG (`ausd_bp_ta_bpr_basis_his.py`)
Python code representing the replacement Airflow workflow.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default settings matching corporate standard run patterns
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 21),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ausd_bp_ta_bpr_basis_his",
    default_args=default_args,
    description="BERT_P_BASISPRODUKT: Extraction & Stage of Instantiated Base Products (GCP Target)",
    schedule_interval=None,  # Typically triggered via corporate orchestration dependencies
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "bert", "basisprodukt", "history"],
) as dag:

    # Task to execute the SQL query stored in GCS or structured template
    process_bpr_basis_his = BigQueryExecuteQueryOperator(
        task_id="process_bpr_basis_his",
        sql="d_ausd_bp_ta_bpr_basis_his.sql",  # Path to SQL file
        use_legacy_sql=False,
        location="EU",
        gcp_conn_id="google_cloud_default",
    )

    process_bpr_basis_his
```