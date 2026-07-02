# MIGRATION DESIGN DOCUMENT: `ausd_bp_ta_bpr_basis_his`

This design document outlines the migration of the legacy UC4 job and its associated KornShell scripts and Oracle SQL scripts into an optimized, cloud-native architecture on Google Cloud Platform (GCP) using **BigQuery** and **Cloud Composer (Airflow)**.

---

## 1. SOURCE TO TARGET REPLACEMENTS & SYSTEM MAPPING

| Legacy Component | Target Platform | Replacement Strategy |
| :--- | :--- | :--- |
| **UC4/Automic Job Scheduler** | **GCP Cloud Composer (Airflow)** | Consolidated into an Airflow DAG (`dw_bert_ausd_bp_ta_bpr_basis_his`). |
| **KornShell Wrappers (`r_*.ksh` / `k_*.ksh`)** | **Airflow Orchestration** | Parameters and execution control logic are handled natively in the Airflow DAG configuration. Inactive legacy files-handling logic (commented-out joins) is discarded. |
| **Oracle Database** | **Google BigQuery** | Target and metadata tables mapped to BigQuery standard tables. |
| **DB Link (`@pcrs1` / `&v_carmen`)** | **BigQuery Cross-Dataset Reference** | The remote Oracle tables accessed via DB Link must be pre-replicated into Google BigQuery standard datasets (e.g., via a CDC pipeline like Fivetran/Qlik or Google Datastream). |
| **Oracle SQL*Plus** | **BigQuery SQL Operator** | Executed using `BigQueryExecuteQueryOperator` within the consolidated DAG. |

---

## 2. LINEAGE, DEPENDENCIES, & EXECUTION ORDER

### Upstream Dependencies (Producers)
1. **`cds.ta_cntrct`** (Replicated source table containing contract statuses and details).
2. **`pds.ta_bpri_com`** (Replicated source table containing product instances).
3. **`isbert_schema.dwtk_meldungen`** (Operational/monitoring table containing run date configurations, specifically for the job `BERT_DROP_TEMP_TABLE`).

### Downstream Consumers
* **`sof.ta_bpr_basis_his`** (Target table populating historical base product instance data for downstream Scoring/BERT analytics engines).

### Consolidating Execution Flow
The legacy execution path involved several manual parameter evaluations across shell wrappers. The modernized GCP execution is streamlined as follows:
```
[Airflow DAG Triggered]
         │
         ▼
[Step 1: Fetch Cutoff Date (v_datum)]
  Query isbert_schema.dwtk_meldungen to get the latest completed operational date
         │
         ▼
[Step 2: Truncate Target Table]
  Execute TRUNCATE on sof.ta_bpr_basis_his to prepare for clean load
         │
         ▼
[Step 3: Extract, Transform & Load]
  Run consolidated INSERT SELECT query from cds.ta_cntrct and pds.ta_bpri_com
         │
         ▼
[Step 4: Commit & Mark DAG Status]
  DAG execution completes and updates Airflow state
```

---

## 3. TARGET FILE PLAN

| Target File Path | Target Language | Source Component File | Purpose |
| :--- | :--- | :--- | :--- |
| `dags/dw_bert_ausd_bp_ta_bpr_basis_his.py` | Python (Airflow DAG) | `DW.BERT_AUSD_BP_TA_BPR_BASIS_HIS.xml`<br>`r_ausd_bp_ta_bpr_basis_his.ksh`<br>`k_ausd_bp_ta_bpr_basis_his.ksh` | Coordinates parameters, variables, dates, and triggers the BigQuery execution. |
| `sql/d_ausd_bp_ta_bpr_basis_his.sql` | BigQuery Standard SQL | `d_ausd_bp_ta_bpr_basis_his.sql` | Contains the complete truncated, date-filtered, and joined transformation query. |

---

## 4. VERBATIM MCP TOOL OUTPUTS (PSEUDO-CODE & TRANSFORMATIONS)

The following raw outputs were generated directly by the migration build engine and are included verbatim for execution and code generation compatibility.

=== Result for vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BPR_BASIS_HIS.xml ===
```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id="dw_bert_ausd_bp_ta_bpr_basis_his",
    default_args=default_args,
    description="BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte",
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "dw", "basisprodukt"],
)

def build_basis_his_sql():
    # Single SQL statement placeholder for the full BigQuery processing logic.
    # Replace the SQL below with the complete transformation logic required by the source job.
    sql = """
    CREATE TABLE IF NOT EXISTS `project.dataset.target_table` AS
    SELECT
        *
    FROM
        `project.dataset.source_table`
    WHERE
        1 = 0
    """
    return sql

def create_bigquery_task():
    # Build the SQL once and execute it with a single BigQuery operator.
    query = build_basis_his_sql()

    return BigQueryExecuteQueryOperator(
        task_id="process_basis_his",
        sql=query,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
    )

# Optional Python task to keep SQL organization modular and explicit.
prepare_sql = PythonOperator(
    task_id="prepare_sql",
    python_callable=build_basis_his_sql,
    dag=dag,
)

# Single BigQuery execution task
process_basis_his = create_bigquery_task()

# Task dependencies
prepare_sql >> process_basis_his
```

=== Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh ===
```python
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="d_ausd_bp_ta_bpr_basis_his_bigquery",
    default_args=default_args,
    description="BigQuery processing DAG for PoolBasisprodukt based on legacy shell logic",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "poolbasisprodukt", "legacy-port"],
) as dag:

    def build_bigquery_sql(**context):
        # Extract runtime parameters passed to the DAG
        dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}
        p_jobkennung = dag_run_conf.get("p_JobKennung", "")
        p_eintragsnr = dag_run_conf.get("p_EintragsNr", "")
        p_stichtag = dag_run_conf.get("p_Stichtag", "")
        p_wiederanlaufwert = dag_run_conf.get("p_wiederanlaufWert", 0)
        p_datum_heute = dag_run_conf.get("p_datum_heute", "")
        p_datum_gestern = dag_run_conf.get("p_datum_gestern", "")

        # Single SQL statement encapsulating the full processing logic
        sql = f"""
        CREATE OR REPLACE TABLE `your_project.your_dataset.PoolBasisprodukt`
        OPTIONS (
          description = 'Initialbefuellung / Datenverarbeitung fuer PoolBasisprodukt'
        ) AS
        WITH
          params AS (
            SELECT
              '{p_jobkennung}' AS p_JobKennung,
              '{p_eintragsnr}' AS p_EintragsNr,
              '{p_stichtag}' AS p_Stichtag,
              CAST({int(p_wiederanlaufwert) if str(p_wiederanlaufwert).isdigit() else 0} AS INT64) AS p_wiederanlaufWert,
              '{p_datum_heute}' AS p_datum_heute,
              '{p_datum_gestern}' AS p_datum_gestern
          ),

          -- Replace the following source tables with the actual BigQuery source tables
          data24 AS (
            SELECT
              key_id,
              col_24_1,
              col_24_2
            FROM `your_project.your_dataset.cibasis_data24`
          ),

          data96 AS (
            SELECT
              key_id,
              col_96_1,
              col_96_2
            FROM `your_project.your_dataset.cibasis_data96`
          ),

          fax AS (
            SELECT
              key_id,
              col_fax_1,
              col_fax_2
            FROM `your_project.your_dataset.cibasis_fax`
          ),

          -- Deduplicate each source by key, mirroring the legacy sort -u behavior
          data24_dedup AS (
            SELECT * EXCEPT(rn)
            FROM (
              SELECT
                d.*,
                ROW_NUMBER() OVER (PARTITION BY key_id ORDER BY key_id) AS rn
              FROM data24 d
            )
            WHERE rn = 1
          ),

          data96_dedup AS (
            SELECT * EXCEPT(rn)
            FROM (
              SELECT
                d.*,
                ROW_NUMBER() OVER (PARTITION BY key_id ORDER BY key_id) AS rn
              FROM data96 d
            )
            WHERE rn = 1
          ),

          fax_dedup AS (
            SELECT * EXCEPT(rn)
            FROM (
              SELECT
                d.*,
                ROW_NUMBER() OVER (PARTITION BY key_id ORDER BY key_id) AS rn
              FROM fax d
            )
            WHERE rn = 1
          ),

          -- Join data24 and data96 with full outer semantics to preserve unmatched rows
          join_24_96 AS (
            SELECT
              COALESCE(d24.key_id, d96.key_id) AS key_id,
              d24.col_24_1,
              d24.col_24_2,
              d96.col_96_1,
              d96.col_96_2
            FROM data24_dedup d24
            FULL OUTER JOIN data96_dedup d96
              ON d24.key_id = d96.key_id
          ),

          -- Join the intermediate result with fax data
          final_join AS (
            SELECT
              COALESCE(j.key_id, f.key_id) AS key_id,
              j.col_24_1,
              j.col_24_2,
              j.col_96_1,
              j.col_96_2,
              f.col_fax_1,
              f.col_fax_2
            FROM join_24_96 j
            FULL OUTER JOIN fax_dedup f
              ON j.key_id = f.key_id
          ),

          -- Add metadata columns to reflect job context and processing dates
          enriched AS (
            SELECT
              key_id,
              col_24_1,
              col_24_2,
              col_96_1,
              col_96_2,
              col_fax_1,
              col_fax_2,
              p_JobKennung,
              p_EintragsNr,
              p_Stichtag,
              p_wiederanlaufWert,
              p_datum_heute,
              p_datum_gestern,
              CURRENT_TIMESTAMP() AS loaded_at
            FROM final_join
            CROSS JOIN params
          )

        SELECT
          *
        FROM enriched
        """

        return BigQueryExecuteQueryOperator(
            task_id="process_poolbasisprodukt",
            sql=sql,
            use_legacy_sql=False,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            location="EU",
        )

    process_task = build_bigquery_sql()

    # Single task DAG; dependencies are explicit for clarity and future extension
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: None,
    )

    start >> process_task
```

=== Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis_his.ksh ===
```python
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id="bereitstellung_basisprodukte_bert",
    default_args=default_args,
    description="Initial provisioning of selected base products for BERT using BigQuery",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "bert", "basisprodukte"],
)

def build_basisprodukte_sql(stichtag: str, wiederanlaufwert: int) -> str:
    """
    Build the full BigQuery SQL statement for the basis product provisioning logic.
    The SQL is kept in a single statement and can be executed by one BigQuery operator.
    """
    return f"""
    -- Target table is created if it does not exist via create_disposition='CREATE_IF_NEEDED'
    -- The query performs the full provisioning logic in one SQL statement.

    DECLARE p_stichtag DATE DEFAULT DATE('{stichtag}');
    DECLARE p_wiederanlaufwert INT64 DEFAULT {wiederanlaufwert};

    -- If no explicit date is provided, use current system date as fallback.
    -- This mirrors the shell script behavior where the system date is used when no Stichtag is set.
    WITH
    params AS (
      SELECT
        p_stichtag AS stichtag,
        p_wiederanlaufwert AS wiederanlaufwert
    ),

    -- Source data selection:
    -- Select records valid for the given cutoff date and loaded before the cutoff date.
    source_data AS (
      SELECT
        *
      FROM `project.dataset.ta_vertrag_cache`
      WHERE DATE(gueltig_von) <= (SELECT stichtag FROM params)
        AND (SELECT stichtag FROM params) < DATE(gueltig_bis)
        AND DATE(ladedatum) < (SELECT stichtag FROM params)
    ),

    -- Apply restart logic:
    -- If a restart value is provided, only keep records with DWH_VERTRAG_ID greater than that value.
    filtered_data AS (
      SELECT
        *
      FROM source_data
      WHERE dwh_vertrag_id > (SELECT wiederanlaufwert FROM params)
    ),

    -- Optional cleanup logic:
    -- Remove rows in the target range that would be replaced by the current run.
    cleanup AS (
      SELECT
        *
      FROM `project.dataset.fos_basisprodukte`
      WHERE dwh_vertrag_id < (SELECT wiederanlaufwert FROM params)
         OR (SELECT wiederanlaufwert FROM params) = 0
    )

    -- Final write:
    -- Replace the target table contents with the cleaned existing rows plus the newly selected rows.
    SELECT
      *
    FROM cleanup

    UNION ALL

    SELECT
      *
    FROM filtered_data
    """

def create_basisprodukte_task():
    """
    Create the single BigQuery task that executes the complete SQL logic.
    """
    sql_query = build_basisprodukte_sql(
        stichtag="{{ dag_run.conf.get('stichtag', ds_nodash) }}",
        wiederanlaufwert="{{ dag_run.conf.get('wiederanlaufwert', 0) }}",
    )

    return BigQueryExecuteQueryOperator(
        task_id="bereitstellung_basisprodukte_bbert_bigquery",
        sql=sql_query,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        bigquery_conn_id="google_cloud_default",
        dag=dag,
    )

# Single task execution
basisprodukte_task = create_basisprodukte_task()

# Task dependencies
basisprodukte_task
```

=== Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_basis_his.sql ===
```python
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago


# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# SQL logic encapsulated in a single Python function
def build_bigquery_sql():
    # Truncate target table to support restart/idempotent execution
    sql = """
    TRUNCATE TABLE `sof.ta_bpr_basis_his`;

    INSERT INTO `sof.ta_bpr_basis_his`
    (
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
    FROM `cds.ta_cntrct` c
    JOIN `pds.ta_bpri_com` bp
      ON c.cntrct_id = bp.cntrct_id
    WHERE c.cntrct_st IN (5, 6)
      AND c.redundant_owner_id = 1
      AND c.insert_at <= DATE('1900-01-01')
      AND (c.modified_at IS NULL OR c.modified_at > DATE('1900-01-01'))
      AND c.valid_from <= DATE('1900-01-01')
      AND (c.valid_to IS NULL OR c.valid_to > DATE('1900-01-01'))
      AND c.is_production = 1
      AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
      AND bp.bpr_id IN (31, 2759, 2800, 2835, 2836, 2837, 3848)
      AND bp.insert_at <= DATE('1900-01-01')
      AND (bp.modified_at IS NULL OR bp.modified_at > DATE('1900-01-01'))
      AND bp.valid_from <= DATE('1900-01-01')
      AND bp.is_production = 1;
    """
    return sql


with DAG(
    dag_id="d_ausd_bp_ta_bpr_basis_his",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    description="BigQuery DAG for d_ausd_bp_ta_bpr_basis_his processing",
    tags=["bigquery", "basisprodukt", "historical"],
) as dag:

    # Build the SQL once in a modular Python function
    sql_query = build_bigquery_sql()

    # Single BigQuery operator executing all SQL logic in one task
    process_basis_history = BigQueryExecuteQueryOperator(
        task_id="process_basis_history",
        sql=sql_query,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
    )

    # Explicit task dependency placeholder for extensibility
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: None,
    )

    start >> process_basis_history
```

---

## 5. RE-ARCHITECTED TARGET BUILD (CONSOLIDATED PLAN)

Rather than maintaining several independent DAGs, the workflow is combined into a single production-ready **Airflow DAG** and a **BigQuery SQL** script to represent the unified UC4 job.

### 5.1 Re-Architected BigQuery SQL Script (`sql/d_ausd_bp_ta_bpr_basis_his.sql`)

```sql
-- Declarations for local script execution and metadata references
DECLARE v_datum DATE;

-- Fetch v_datum from monitoring metadata table, mimicking step00
SET v_datum = (
  SELECT COALESCE(DATE(MAX(timecreated)), DATE('1900-01-01'))
  FROM `gcp-dwh-prod.isbert_schema.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 01: Truncate local target table to handle restarts idempotently
TRUNCATE TABLE `gcp-dwh-prod.sof.ta_bpr_basis_his`;

-- Step 03b: Populate target table using date filter criteria
INSERT INTO `gcp-dwh-prod.sof.ta_bpr_basis_his`
(
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
  -- Safe concatenation of components with COALESCE to mimic Oracle string concatenation behavior
  CONCAT(
    IFNULL(bp.iccid_mi, ''), '-', 
    IFNULL(bp.iccid_ii, ''), '-', 
    IFNULL(bp.iccid_iai, ''), '-', 
    IFNULL(bp.iccid_nr, ''), '-', 
    IFNULL(bp.iccid_cd, '')
  ) AS iccid,
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
FROM `gcp-dwh-prod.cds.ta_cntrct` c
JOIN `gcp-dwh-prod.pds.ta_bpri_com` bp
  ON c.cntrct_id = bp.cntrct_id
WHERE c.cntrct_st IN (5, 6)                         -- Active and deactivated-reactivable
  AND c.redundant_owner_id = 1                      -- Exclude Service Provider Contracts
  AND DATE(c.insert_at) <= v_datum
  AND (c.modified_at IS NULL OR DATE(c.modified_at) > v_datum)
  AND DATE(c.valid_from) <= v_datum
  AND (c.valid_to IS NULL OR DATE(c.valid_to) > v_datum)
  AND c.is_production = 1
  AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
  -- Filter on target base product IDs
  AND bp.bpr_id IN (31, 2759, 2800, 2835, 2836, 2837, 3848)
  AND DATE(bp.insert_at) <= v_datum
  AND (bp.modified_at IS NULL OR DATE(bp.modified_at) > v_datum)
  AND DATE(bp.valid_from) <= v_datum
  AND bp.is_production = 1;
```

### 5.2 Consolidated Airflow Orchestration DAG (`dags/dw_bert_ausd_bp_ta_bpr_basis_his.py`)

```python
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Retrieve environment-specific variables
PROJECT_ID = os.getenv("GCP_PROJECT", "gcp-dwh-prod")
CONN_ID = "google_cloud_default"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_bert_ausd_bp_ta_bpr_basis_his",
    default_args=default_args,
    description="Orchestrator for BERT Base Products Historical Processing",
    schedule_interval="0 4 * * *",  # Run daily at 04:00 AM
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "bert", "dwh"],
) as dag:

    # Load SQL query file from target directory structure
    sql_file_path = os.path.join(
        os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags"),
        "sql/d_ausd_bp_ta_bpr_basis_his.sql"
    )

    execute_conversion_query = BigQueryExecuteQueryOperator(
        task_id="execute_ausd_bp_ta_bpr_basis_his",
        sql="sql/d_ausd_bp_ta_bpr_basis_his.sql",  # Points to SQL inside templated paths
        use_legacy_sql=False,
        bigquery_conn_id=CONN_ID,
        write_disposition="WRITE_TRUNCATE",  # Keeps updates clean
        create_disposition="CREATE_IF_NEEDED",
    )

    execute_conversion_query
```

---

## 6. ENVIRONMENT-SPECIFIC CONFIGURATIONS & VALUES

For local deployments and environments, variables and projects can be parameterized inside the DAG code or via Airflow Connection settings.

* **Development Project ID**: `gcp-dwh-dev`
* **Production Project ID**: `gcp-dwh-prod`
* **GCP Location**: `EU` (or specific regional location mirroring legacy systems).
* **Airflow Connection ID**: `google_cloud_default`
* **Metadata Schema**: `isbert_schema`
* **Source Datasets**: `cds` and `pds`
* **Target Dataset**: `sof` (contains table `ta_bpr_basis_his`).

---

## 7. RISKS, CHALLENGES & MANUAL MIGRATION STEPS

### 1. Database Link Migration Strategy
* **Risk**: The legacy query uses `&v_carmen` which translates to `@pcrs1` (Oracle DB Link).
* **Mitigation**: The tables `cds$ta_cntrct` and `pds$ta_bpri_com` must be migrated or synced dynamically to Google BigQuery datasets prior to the trigger of this job. Direct cross-database joins are not natively supported in BigQuery unless mapped into separate Google Cloud datasets (e.g., `cds` and `pds`).

### 2. Concatenation and Null Handling
* **Risk**: In Oracle SQL, `NULL || '-' || NULL` outputs `'-'`. In BigQuery SQL, standard `CONCAT` with `NULL` yields `NULL` (null propagation).
* **Mitigation**: The consolidated SQL implementation uses `CONCAT(IFNULL(...))` or `COALESCE` to prevent null propagation during the execution of string formatting logic.

### 3. Verification of `dwtk_meldungen` Metadata
* **Risk**: If the metadata generation workflow (e.g., for the job `BERT_DROP_TEMP_TABLE`) is missing in GCP, the script will fall back to `'1900-01-01'`.
* **Mitigation**: Confirm that the upstream orchestration system updates the target table `isbert_schema.dwtk_meldungen` with dynamic values, or replace the lookup with the Airflow run parameters (`{{ ds }}`) if metadata tables are decommissioned.