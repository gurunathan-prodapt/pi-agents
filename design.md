# Migration Design — ausd_bp_ta_bpr_basis

## 1. Purpose & Scope
The job **`ausd_bp_ta_bpr_basis`** prepares and processes customer base product contract instance data (such as Fax, Data24, Twin Card, Twin Bill Privat, connection details, and MultiSIM card configurations) for BERT (*Forderungsscoring* / Receivables Scoring and Dunning). 

It aggregates contract and SIM card info active on a given key date (`Stichtag` / `v_datum`) from remote sources. In the legacy environment, this is triggered via UC4 UNIX jobs executing KornShell wrapper scripts that execute Oracle SQL\*Plus commands. 

The scope of this migration involves re-architecting the entire process to run natively in **Google Cloud Platform (GCP)**:
- **Orchestration**: Replaced UC4 workflows and shell parameters with a **Google Cloud Composer (Airflow)** DAG.
- **Database/Storage**: Re-platformed Oracle tables to **BigQuery** and eliminated the DB Link (`@pcrs1`) by utilizing replicated datasets.
- **Transformation Logic**: Ported complex Oracle SQL\*Plus scripts directly to native **BigQuery standard SQL**.

---

## 2. Source Inventory
The following source components make up this job:

| Original Source Path & File Name | Tech Platform | Complexity Tier | Automation Bucket | Legacy Role | Target Re-Platforming Strategy |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `DW.BERT_AUSD_BP_TA_BPR_BASIS_HIS.xml` | UC4 XML | Medium | Semi-Automated | Historic Job trigger | Replaced by Task 1 in the new Airflow DAG |
| `DW.BERT_AUSD_BP_TA_BPR_BASIS.xml` | UC4 XML | Medium | Semi-Automated | Main Job trigger | Replaced by Task 2 in the new Airflow DAG |
| `r_ausd_bp_ta_bpr_basis_his.ksh` | KornShell | Medium | Semi-Automated | Execution Wrapper | Replaced by Airflow BigQuery execution parameters |
| `r_ausd_bp_ta_bpr_basis.ksh` | KornShell | Medium | Semi-Automated | Execution Wrapper | Replaced by Airflow BigQuery execution parameters |
| `k_ausd_bp_ta_bpr_basis_his.ksh` | KornShell | Medium | Semi-Automated | Parameter Checker / Controller | Retired (validated natively in Airflow & BigQuery SQL) |
| `k_ausd_bp_ta_bpr_basis.ksh` | KornShell | Medium | Semi-Automated | Parameter Checker / Controller | Retired (validated natively in Airflow & BigQuery SQL) |
| `d_ausd_bp_ta_bpr_basis_his.sql` | Oracle SQL | Medium | Semi-Automated | SQL ETL Script (Historical) | Ported to native BigQuery SQL script |
| `d_ausd_bp_ta_bpr_basis.sql` | Oracle SQL | Medium | Semi-Automated | SQL ETL Script (Main/Daily) | Ported to native BigQuery SQL script |

---

## 3. Target Architecture
The migrated BigQuery layout is split across two logical datasets to ensure decoupling between raw landing sources and target analytics tables.

### 3.1 Dataset Layout
1. **`src_carmen`** (Source Landing Zone):
   - Host replicated tables from the external source system (`@pcrs1` database link replacement):
     - `cds$ta_cntrct`
     - `pds$ta_bpri_com`
     - `rma$ta_sim`
     - `rma$ta_sim_card_type`
2. **`core_bert`** (Target Analytics Layer):
   - Stores processed customer scoring schemas:
     - `dwtk_meldungen` (Configuration / log parameters)
     - `sof$ta_bpr_basis_his` (Historical output table)
     - `sof$ta_sim` (SIM instance target table)
     - `sof$ta_bpr_basis` (Final consolidated base product table)

### 3.2 Component Architecture
* **Orchestration**: A unified Google Cloud Composer (Airflow) DAG named `dag_ausd_bp_ta_bpr_basis` runs sequentially.
* **Security & Execution**: Airflow utilizes a GCP Service Account with `roles/bigquery.jobUser` and `roles/bigquery.dataEditor` permissions.

---

## 4. Data Flow & Lineage

The end-to-end lineage flow executes sequentially. The historical task (`_HIS`) must complete before the final consolidation run.

```
[src_carmen.cds$ta_cntrct]     ──┐
                                 ├─► [core_bert.sof$ta_bpr_basis_his] ──┐
[src_carmen.pds$ta_bpri_com]  ──┘                                       │
                                                                         ├─► [core_bert.sof$ta_bpr_basis]
[src_carmen.rma$ta_sim]           ──┐                                    │
                                    ├─► [core_bert.sof$ta_sim] ──────────┘
[src_carmen.rma$ta_sim_card_type] ──┘
```

### Execution Steps
1. **`Task_1_Extract_His_SQL`**: Execute the historical logic. Read from `cds$ta_cntrct` and `pds$ta_bpri_com`, truncate and write back results to `sof$ta_bpr_basis_his`.
2. **`Task_2_Extract_Sim_SQL`**: Read from `rma$ta_sim` and `rma$ta_sim_card_type`, truncate and load `sof$ta_sim`.
3. **`Task_3_Consolidate_Basis_SQL`**: Join `sof$ta_bpr_basis_his` and `sof$ta_sim` using analytical window functions, then write the final daily set into target `sof$ta_bpr_basis`.

---

## 5. Transformation Logic

### 5.1 Parameter Resolution (`v_datum`)
In BigQuery, substitution variables are resolved dynamically using native variables and scripting blocks.
```sql
DECLARE v_datum STRING;

SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(timecreated)), '19000101')
  FROM `core_bert.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);
```

### 5.2 SQL Translation 1: Historical Base Product Extract
**Source File:** `d_ausd_bp_ta_bpr_basis_his.sql`  
**Target Action:** Natively executed as a query statement in BigQuery.

```sql
-- Target BigQuery Script: d_ausd_bp_ta_bpr_basis_his.sql
DECLARE v_datum STRING;
DECLARE v_datum_date DATE;

SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(timecreated)), '19000101')
  FROM `core_bert.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

SET v_datum_date = PARSE_DATE('%Y%m%d', v_datum);

-- Step 1: Truncate local target table
TRUNCATE TABLE `core_bert.sof$ta_bpr_basis_his`;

-- Step 2: Insert historical basis products
INSERT INTO `core_bert.sof$ta_bpr_basis_his` (
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
  bp.cntrct_id,
  bp.bpr_id,
  bp.bpri_com_id,
  CONCAT(bp.iccid_mi, '-', bp.iccid_ii, '-', bp.iccid_iai, '-', bp.iccid_nr, '-', bp.iccid_cd) AS iccid,
  bp.imsi_mcc,
  bp.imsi_mnc,
  bp.imsi_hlr,
  bp.imsi_si,
  bp.cntrct_id_ref,
  bp.valid_from,
  bp.valid_to,
  bp.modified_at,
  bp.insert_at,
  bp.slave_number,
  bp.eid
FROM `src_carmen.cds$ta_cntrct` c
INNER JOIN `src_carmen.pds$ta_bpri_com` bp ON c.cntrct_id = bp.cntrct_id
WHERE c.cntrct_st IN (5, 6)
  AND c.redundant_owner_id = 1
  AND c.insert_at <= v_datum_date
  AND (c.modified_at IS NULL OR c.modified_at > v_datum_date)
  AND c.valid_from <= v_datum_date
  AND (c.valid_to IS NULL OR c.valid_to > v_datum_date)
  AND c.is_production = 1
  AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
  -- Filter on target base product IDs
  AND bp.bpr_id IN (
    31,   -- tnv
    2759, -- twin-card
    2800, -- twin-bill-privat
    2835, -- da-anschluss
    2836, -- vda-anschluss
    2837, -- tk-anschluss
    3848  -- MultiSIM
  )
  AND bp.insert_at <= v_datum_date
  AND (bp.modified_at IS NULL OR bp.modified_at > v_datum_date)
  AND bp.valid_from <= v_datum_date
  AND bp.is_production = 1;
```

### 5.3 SQL Translation 2: Consolidated Base Product Run
**Source File:** `d_ausd_bp_ta_bpr_basis.sql`  
**Target Action:** Executed in BigQuery to resolve SIM card details and consolidate base products.

```sql
-- Target BigQuery Script: d_ausd_bp_ta_bpr_basis.sql
DECLARE v_datum STRING;
DECLARE v_datum_date DATE;

SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(timecreated)), '19000101')
  FROM `core_bert.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

SET v_datum_date = PARSE_DATE('%Y%m%d', v_datum);

-- Step 1: Truncate temporary target tables
TRUNCATE TABLE `core_bert.sof$ta_sim`;
TRUNCATE TABLE `core_bert.sof$ta_bpr_basis`;

-- Step 2: Populate local SIM table
INSERT INTO `core_bert.sof$ta_sim` (
  iccid,
  sim_card_type_id,
  card_type_name
)
SELECT 
  CONCAT(sim.iccid_mi, '-', sim.iccid_ii, '-', sim.iccid_iai, '-', sim.iccid_nr, '-', sim.iccid_cd) AS iccid,
  sim.sim_card_type_id,
  card.card_type_name
FROM `src_carmen.rma$ta_sim` sim
INNER JOIN `src_carmen.rma$ta_sim_card_type` card ON card.sim_card_type_id = sim.sim_card_type_id
WHERE sim.insert_at <= v_datum_date
  AND (sim.modified_at IS NULL OR sim.modified_at > v_datum_date)
  AND sim.valid_from <= v_datum_date
  AND (sim.valid_to IS NULL OR sim.valid_to > v_datum_date)
  AND card.insert_at <= v_datum_date
  AND (card.modified_at IS NULL OR card.modified_at > v_datum_date);

-- Step 3: Populate target base products consolidated with active SIM status
INSERT INTO `core_bert.sof$ta_bpr_basis` (
  cntrct_id,
  bpr_id,
  bpr_instance_id,
  iccid,
  imsi_mcc,
  imsi_mnc,
  imsi_hlr,
  imsi_si,
  valid_to,
  slave_number,
  e_id,
  card_type_name
)
SELECT 
  bp.cntrct_id,
  bp.bpr_id,
  bp.bpri_com_id AS bpr_instance_id,
  bp.iccid,
  bp.imsi_mcc,
  bp.imsi_mnc,
  bp.imsi_hlr,
  bp.imsi_si,
  COALESCE(bp.valid_to, DATE '4712-12-31') AS valid_to,
  bp.slave_number,
  bp.e_id,
  sim.card_type_name
FROM (
  SELECT 
    bp1.*,
    MAX(COALESCE(bp1.valid_to, DATE '4712-12-31')) OVER (PARTITION BY bp1.cntrct_id, bp1.bpr_id) AS max_valid_to
  FROM `core_bert.sof$ta_bpr_basis_his` bp1
) bp
LEFT OUTER JOIN `core_bert.sof$ta_sim` sim ON bp.iccid = sim.iccid
WHERE COALESCE(bp.valid_to, DATE '4712-12-31') = bp.max_valid_to;
```

---

## 6. External Dependencies
* **Oracle DB Link Removal (`@pcrs1`)**: The dependency on the Oracle remote database link is entirely removed. Tables `cds$ta_cntrct`, `pds$ta_bpri_com`, `rma$ta_sim`, and `rma$ta_sim_card_type` must be replicated into BigQuery (`src_carmen` dataset) prior to executing this pipeline (configured via Datastream or standard transfer workflows).
* **Environment Configuration (`.dw_init`, `gestern.ksh`)**: All variables are resolved dynamically in SQL or injected natively through Cloud Composer runtime parameters.
* **Logging System (`f_alis_msgerr.ksh`, `DW.BERT_LESE_LOG`)**: Migrated to native Cloud Logging via standard logging frameworks in Composer (Python logger in Airflow Tasks).

---

## 7. Unresolved / Risks
* **Data Replication Sync Risk**: Since the target table matches records against `cds$ta_cntrct` and other primary systems, data must be fully loaded into the BigQuery staging layer (`src_carmen`) before executing this DAG. Latency in replication will lead to data inconsistencies.
* **Data Volume Scale**: Standard window analytical operations (`MAX() OVER (PARTITION BY ...)`) are highly scalable on BigQuery but require appropriate partition structures if target scale increases dramatically over time.

---

## 8. Build Plan

This section contains the step-by-step implementation order.

1. **Staging Schema Creation**: Deploy BigQuery landing and core datasets:
   - Create `src_carmen` and `core_bert` datasets.
2. **Replication Setup**: Set up replication pipelines for Carmen tables (`cds$ta_cntrct`, `pds$ta_bpri_com`, `rma$ta_sim`, `rma$ta_sim_card_type`) pointing to target tables in `src_carmen`.
3. **Deploy Target DDL**: Create destination schemas for `sof$ta_bpr_basis_his`, `sof$ta_sim`, and `sof$ta_bpr_basis`.
4. **Deploy SQL Scripts**: Save `d_ausd_bp_ta_bpr_basis_his.sql` and `d_ausd_bp_ta_bpr_basis.sql` inside Cloud Storage for reference by Cloud Composer.
5. **Airflow DAG Deployment**: Deploy `dag_ausd_bp_ta_bpr_basis.py` orchestration script inside Composer.

### Cloud Composer Workflow Design (Airflow DAG)
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 21),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_ausd_bp_ta_bpr_basis',
    default_args=default_args,
    description='Prepares instantiated base products for BERT scoring',
    schedule_interval='0 2 * * *',  # Runs daily at 2:00 AM
    catchup=False,
) as dag:

    # Task 1: Historical Data Processing 
    run_historical_sql = BigQueryInsertJobOperator(
        task_id='run_historical_sql',
        configuration={
            "query": {
                "query": "d_ausd_bp_ta_bpr_basis_his.sql", # Ref to GCS file or template query
                "useLegacySql": False,
            }
        }
    )

    # Task 2: Consolidated Run (Includes SIM and final Basis Product load)
    run_consolidation_sql = BigQueryInsertJobOperator(
        task_id='run_consolidation_sql',
        configuration={
            "query": {
                "query": "d_ausd_bp_ta_bpr_basis.sql", # Ref to GCS file or template query
                "useLegacySql": False,
            }
        }
    )

    run_historical_sql >> run_consolidation_sql
```