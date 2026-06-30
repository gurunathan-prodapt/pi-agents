# MIGRATION DESIGN DOCUMENT: `ausd_bp_ta_bpr_instance`

This document details the blueprint and specifications for migrating the legacy UC4, KornShell, and Oracle SQL*Plus job `ausd_bp_ta_bpr_instance` to an enterprise Cloud Data Platform running on Google Cloud Platform (GCP) with **Apache Airflow** (Cloud Composer) and **BigQuery**.

---

## 1. COMPREHENSIVE CONTEXT & SYSTEM OVERVIEW (ADDITIONAL CONTEXT)

### 1.1 Lineage & Execution Chain
*   **Upstream Producers / Dependencies**:
    *   `isbert_schema.dwtk_meldungen`: Reads from this tracking table to determine the watermark value `v_datum` based on the status entry for `BERT_DROP_TEMP_TABLE`.
    *   `cds$ta_cntrct` (sourced via remote link `@pcrs1` from Carmen source database): Master contract catalog containing contract headers.
    *   `pds$ta_bpri_com` (sourced via remote link `@pcrs1` from Carmen source database): Source of the basis product instance details.
*   **Downstream Consumers**:
    *   The target table `sof$ta_bpr_instance` is subsequently read by downstream BERT scoring models, scoring engines, and reporting tables.
*   **Legacy Execution Order**:
    1.  **UC4**: Triggers `r_ausd_bp_ta_bpr_instance.ksh`.
    2.  **Orchestrator (`r_...ksh`)**: Sets environment, calculates default business date (Stichtag), and registers the running job status in `dwtk_meldungen`.
    3.  **Kernel Wrapper (`k_...ksh`)**: Validates the parameters, calls date helpers (`gestern.ksh`), and runs the SQL*Plus client wrapper.
    4.  **SQL Script (`d_...sql`)**: Establishes session settings, executes dynamic DDL/DML, and truncates/inserts records.

### 1.2 Target System Replacements
*   **Legacy Scheduler**: UC4/Automic XML configurations are fully replaced by a pythonic **Apache Airflow DAG**.
*   **Legacy DB Link (`@pcrs1`)**: Replaced by **BigQuery Federated Queries**, **BigQuery Omni**, or pre-replicated tables loaded using modern ELT pipelines (e.g., Google Cloud Data Fusion, Fivetran, or Storage Transfer Service) in unified staging datasets (`cds` and `pds`).
*   **KornShell Wrapper scripts**: Replaced by standard operators within the Apache Airflow framework, specifically the **`BigQueryExecuteQueryOperator`**. This eliminates shell dependencies, local file storage for logs, and `sqlplus` dependencies.

### 1.3 Cross-File Dependencies & Global Logic
*   **Shared Dynamic Variable (`v_datum`)**:
    The legacy Oracle SQL script uses `SELECT MAX(timecreated)` from `dwtk_meldungen` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` to define a substitution variable `v_datum` dynamically.
    *   *BigQuery Pattern*: This is resolved in the migrated pipeline using native BigQuery script syntax with standard script variables (`DECLARE v_datum STRING; SET v_datum = (...)`), ensuring the script remains atomic and self-contained.
*   **String Formatting**:
    Oracle's string concatenation `bp.iccid_mi||'-'||bp.iccid_ii...` maps to BigQuery's `CONCAT()` function. Handling potential `NULL` values during migration should employ `COALESCE` to prevent the final concatenated string from evaluating to `NULL`.

---

## 2. TARGET FILE PLAN

The table below outlines the generated target files, their paths in the new repository, the programming language utilized, and their corresponding legacy source files.

| Target Relative Path | Target Language | Source Component File | Description |
| :--- | :--- | :--- | :--- |
| `dags/dag_ausd_bp_ta_bpr_instance.py` | Python (Airflow DAG) | `DW.BERT_AUSD_BP_TA_BPR_INSTANCE.xml`<br>`r_ausd_bp_ta_bpr_instance.ksh`<br>`k_ausd_bp_ta_bpr_instance.ksh` | Consolidated DAG definition that schedules and orchestrates the migration execution flow. Handles execution tasks and parameters. |
| `sql/d_ausd_bp_ta_bpr_instance.sql` | BigQuery SQL (Standard SQL) | `d_ausd_bp_ta_bpr_instance.sql` | Pure SQL/Scripting file containing table truncation and insert-select logic, using variables to find `v_datum`. |

---

## 3. VERBATIM MCP TOOL OUTPUTS

Below are the complete, unmodified migration design artifacts and code representations returned by the code translation engines for each source file.

### ==================== Result for `DW.BERT_AUSD_BP_TA_BPR_INSTANCE.xml` ====================
```python
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# DAG default arguments
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
    dag_id="dw_bert_ausd_bp_ta_bpr_instance",
    default_args=default_args,
    description="BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte",
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "dw", "bert", "basisprodukt"],
)

def process_instantiated_base_products():
    # Single SQL statement encapsulating the full BigQuery processing logic.
    # The target table is created if it does not exist via CREATE_IF_NEEDED.
    sql = """
    CREATE TABLE IF NOT EXISTS `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.AUSD_BP_TA_BPR_INSTANCE` AS
    WITH source_data AS (
        SELECT
            *
        FROM `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.BPR_INSTANCE_SOURCE`
    ),
    filtered_data AS (
        SELECT
            *
        FROM source_data
        WHERE 1 = 1
    ),
    enriched_data AS (
        SELECT
            fd.*,
            CURRENT_TIMESTAMP() AS load_ts
        FROM filtered_data fd
    )
    SELECT
        *
    FROM enriched_data
    """
    return BigQueryExecuteQueryOperator(
        task_id="process_instantiated_base_products",
        sql=sql,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
    )

task = process_instantiated_base_products()
```

### ==================== Result for `k_ausd_bp_ta_bpr_instance.ksh` ====================
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
def build_poolbasisprodukt_sql(stichtag: str, job_kennung: str, eintrags_nr: str, wiederanlauf_wert: int = 0) -> str:
    """
    Build the full BigQuery SQL statement for PoolBasisprodukt processing.
    The SQL is kept as a single statement to be executed by one BigQuery operator.
    """
    sql = f"""
    -- Create or replace the target table if needed and populate it in one statement
    CREATE TABLE IF NOT EXISTS `your_project.your_dataset.PoolBasisprodukt` (
        job_kennung STRING,
        eintrags_nr STRING,
        stichtag STRING,
        wiederanlauf_wert INT64,
        processed_at TIMESTAMP,
        source_record_count INT64
    )
    AS
    SELECT
        '{job_kennung}' AS job_kennung,
        '{eintrags_nr}' AS eintrags_nr,
        '{stichtag}' AS stichtag,
        {int(wiederanlauf_wert)} AS wiederanlauf_wert,
        CURRENT_TIMESTAMP() AS processed_at,
        COUNT(1) AS source_record_count
    FROM `your_project.your_dataset.source_table`
    WHERE FORMAT_DATE('%d%m%Y', DATE(_PARTITIONTIME)) = '{stichtag}';
    """
    return sql


# Python callable to prepare the SQL string
def prepare_sql(**context):
    # Parameters can be passed via DAG run conf or replaced with static values
    dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}

    stichtag = dag_run_conf.get("stichtag", "01012025")
    job_kennung = dag_run_conf.get("job_kennung", "DEFAULT_JOB")
    eintrags_nr = dag_run_conf.get("eintrags_nr", "1")
    wiederanlauf_wert = dag_run_conf.get("wiederanlauf_wert", 0)

    return build_poolbasisprodukt_sql(
        stichtag=stichtag,
        job_kennung=job_kennung,
        eintrags_nr=eintrags_nr,
        wiederanlauf_wert=wiederanlauf_wert,
    )


with DAG(
    dag_id="d_ausd_bp_ta_bpr_instance_bigquery",
    default_args=default_args,
    description="BigQuery processing DAG for PoolBasisprodukt",
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "poolbasisprodukt"],
) as dag:

    # Prepare the SQL statement in a single Python function
    prepare_sql_task = PythonOperator(
        task_id="prepare_sql",
        python_callable=prepare_sql,
        provide_context=True,
    )

    # Execute the full SQL logic using a single BigQuery operator
    run_bigquery_task = BigQueryExecuteQueryOperator(
        task_id="run_poolbasisprodukt_processing",
        sql="{{ ti.xcom_pull(task_ids='prepare_sql') }}",
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        location="EU",
        gcp_conn_id="google_cloud_default",
    )

    # Set task dependency
    prepare_sql_task >> run_bigquery_task
```

### ==================== Result for `r_ausd_bp_ta_bpr_instance.ksh` ====================
```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def build_bigquery_sql(stichtag=None, wiederanlaufwert=0):
    # SQL logic for initial provisioning of selected base products for BERT
    # The query uses a single statement and can create the target table if needed.
    stichtag_expr = f"DATE(PARSE_DATE('%d%m%Y', '{stichtag}'))" if stichtag else "CURRENT_DATE()"
    wiederanlaufwert = int(wiederanlaufwert or 0)

    sql = f"""
    CREATE TABLE IF NOT EXISTS `your_project.your_dataset.bert_basisprodukte`
    AS
    WITH source_data AS (
        SELECT
            *
        FROM `your_project.your_dataset.contract_cache`
        WHERE
            DATE(gueltig_von) <= {stichtag_expr}
            AND {stichtag_expr} < DATE(gueltig_bis)
            AND DATE(ladedatum) < {stichtag_expr}
            AND dwh_vertrag_id > {wiederanlaufwert}
    ),
    deduplicated AS (
        SELECT
            *
        FROM source_data
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY dwh_vertrag_id
            ORDER BY ladedatum DESC
        ) = 1
    )
    SELECT
        *
    FROM deduplicated
    """
    return sql


def create_bigquery_task(**context):
    # Build the SQL in Python so the DAG remains modular and easy to maintain.
    dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}
    stichtag = dag_run_conf.get("stichtag")
    wiederanlaufwert = dag_run_conf.get("wiederanlaufwert", 0)

    sql_query = build_bigquery_sql(stichtag=stichtag, wiederanlaufwert=wiederanlaufwert)

    return BigQueryExecuteQueryOperator(
        task_id="process_bert_basisprodukte",
        sql=sql_query,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud_default",
        location="EU",
    )


with DAG(
    dag_id="bert_basisprodukte_bereitstellung",
    default_args=default_args,
    description="Initial provisioning of selected base products for BERT using BigQuery",
    schedule_interval=None,
    catchup=False,
    tags=["bert", "bigquery", "basisprodukte"],
) as dag:

    # Optional Python task to prepare the SQL and keep the DAG modular.
    prepare_sql = PythonOperator(
        task_id="prepare_sql",
        python_callable=lambda **context: build_bigquery_sql(
            stichtag=(context.get("dag_run").conf or {}).get("stichtag") if context.get("dag_run") else None,
            wiederanlaufwert=(context.get("dag_run").conf or {}).get("wiederanlaufwert", 0) if context.get("dag_run") else 0,
        ),
        provide_context=True,
    )

    # Single BigQuery operator executes the full SQL logic.
    process_bert_basisprodukte = BigQueryExecuteQueryOperator(
        task_id="process_bert_basisprodukte",
        sql="{{ ti.xcom_pull(task_ids='prepare_sql') }}",
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud_default",
        location="EU",
    )

    prepare_sql >> process_bert_basisprodukte
```

### ==================== Result for `d_ausd_bp_ta_bpr_instance.sql` ====================
```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# SQL logic encapsulated in a single function
def build_bq_sql():
    # Single BigQuery SQL statement that truncates and reloads the target table
    sql = """
    -- Clear the target table before reloading data
    TRUNCATE TABLE `sof.ta_bpr_instance`;

    -- Insert the latest valid basis product instances into the target table
    INSERT INTO `sof.ta_bpr_instance`
    (
      CNTRCT_ID,
      BPR_ID,
      BPR_INSTANCE_ID,
      ICCID,
      IMSI_MCC,
      IMSI_MNC,
      IMSI_HLR,
      IMSI_SI,
      CNTRCT_ID_REF
    )
    SELECT
      bp.cntrct_id,
      bp.bpr_id,
      bp.bpri_com_id AS bpr_instance_id,
      CONCAT(
        bp.iccid_mi, '-',
        bp.iccid_ii, '-',
        bp.iccid_iai, '-',
        bp.iccid_nr, '-',
        bp.iccid_cd
      ) AS iccid,
      bp.imsi_mcc,
      bp.imsi_mnc,
      bp.imsi_hlr,
      bp.imsi_si,
      bp.cntrct_id_ref
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
      AND bp.insert_at <= DATE('1900-01-01')
      AND (bp.modified_at IS NULL OR bp.modified_at > DATE('1900-01-01'))
      AND bp.valid_from <= DATE('1900-01-01')
      AND (bp.valid_to IS NULL OR bp.valid_to > DATE('1900-01-01'))
      AND bp.is_production = 1;
    """
    return sql

# DAG definition
with DAG(
    dag_id="d_ausd_bp_bpr_instance",
    default_args=default_args,
    description="BigQuery processing for basis product BPR instances",
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "basisprodukt"],
) as dag:

    # Single BigQuery operator executing the full SQL logic
    process_bpr_instance = BigQueryExecuteQueryOperator(
        task_id="process_bpr_instance",
        sql=build_bq_sql(),
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        location="EU",
    )

    # Task dependency placeholder for extensibility
    process_bpr_instance

dag
```

---

## 4. INTEGRATED PRODUCTION IMPLEMENTATION PLAN

To create a singular, optimized, and maintainable unit of work, the individual scripts are resolved into a single BigQuery script wrapper in Apache Airflow.

### 4.1 Production BigQuery SQL Script (`sql/d_ausd_bp_ta_bpr_instance.sql`)
Instead of hardcoding `1900-01-01`, this script retrieves `v_datum` dynamically from the tracking table, mimicking the Oracle runtime execution.

```sql
DECLARE v_datum DATE;

-- Retrieve dynamic watermark date
SET v_datum = (
  SELECT COALESCE(DATE(MAX(timecreated)), DATE('1900-01-01'))
  FROM `${GCP_PROJECT}.${ISBERT_DATASET}.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Truncate existing data to prevent duplicates on rerun
TRUNCATE TABLE `${GCP_PROJECT}.${SOF_DATASET}.ta_bpr_instance`;

-- Populate the target table
INSERT INTO `${GCP_PROJECT}.${SOF_DATASET}.ta_bpr_instance`
(
  CNTRCT_ID,
  BPR_ID,
  BPR_INSTANCE_ID,
  ICCID,
  IMSI_MCC,
  IMSI_MNC,
  IMSI_HLR,
  IMSI_SI,
  CNTRCT_ID_REF
)
SELECT
  bp.cntrct_id,
  bp.bpr_id,
  bp.bpri_com_id AS bpr_instance_id,
  CONCAT(
    COALESCE(bp.iccid_mi, ''), '-',
    COALESCE(bp.iccid_ii, ''), '-',
    COALESCE(bp.iccid_iai, ''), '-',
    COALESCE(bp.iccid_nr, ''), '-',
    COALESCE(bp.iccid_cd, '')
  ) AS iccid,
  bp.imsi_mcc,
  bp.imsi_mnc,
  bp.imsi_hlr,
  bp.imsi_si,
  bp.cntrct_id_ref
FROM `${GCP_PROJECT}.${CDS_DATASET}.ta_cntrct` c
JOIN `${GCP_PROJECT}.${PDS_DATASET}.ta_bpri_com` bp
  ON c.cntrct_id = bp.cntrct_id
WHERE c.cntrct_st IN (5, 6)
  AND c.redundant_owner_id = 1
  AND DATE(c.insert_at) <= v_datum
  AND (c.modified_at IS NULL OR DATE(c.modified_at) > v_datum)
  AND DATE(c.valid_from) <= v_datum
  AND (c.valid_to IS NULL OR DATE(c.valid_to) > v_datum)
  AND c.is_production = 1
  AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
  AND DATE(bp.insert_at) <= v_datum
  AND (bp.modified_at IS NULL OR DATE(bp.modified_at) > v_datum)
  AND DATE(bp.valid_from) <= v_datum
  AND (bp.valid_to IS NULL OR DATE(bp.valid_to) > v_datum)
  AND bp.is_production = 1;
```

### 4.2 Production Airflow DAG (`dags/dag_ausd_bp_ta_bpr_instance.py`)
This consolidated DAG runs the compiled BigQuery script above, avoiding the execution of shell script loops and local temporary files.

```python
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.models import Variable

# Fetch environment-specific configuration from Airflow Variables
GCP_PROJECT = Variable.get("gcp_project_id", default_var="gcp-dwh-prod")
LOCATION = Variable.get("gcp_location", default_var="EU")

# Read schema names dynamically
ISBERT_DATASET = Variable.get("isbert_dataset", default_var="isbert_schema")
SOF_DATASET = Variable.get("sof_dataset", default_var="sof")
CDS_DATASET = Variable.get("cds_dataset", default_var="cds")
PDS_DATASET = Variable.get("pds_dataset", default_var="pds")

default_args = {
    "owner": "data_migration_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_bert_ausd_bp_ta_bpr_instance",
    default_args=default_args,
    description="Orchestrates BigQuery population of 'ta_bpr_instance' replacing UC4, KSH, and SQL*Plus processes.",
    schedule_interval="0 4 * * *",  # Run daily at 04:00 AM UTC
    catchup=False,
    tags=["bigquery", "bert", "basisprodukt", "sof"],
) as dag:

    # Define the dynamic SQL command incorporating runtime configurations
    sql_script = f"""
    DECLARE v_datum DATE;

    SET v_datum = (
      SELECT COALESCE(DATE(MAX(timecreated)), DATE('1900-01-01'))
      FROM `{GCP_PROJECT}.{ISBERT_DATASET}.dwtk_meldungen`
      WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    TRUNCATE TABLE `{GCP_PROJECT}.{SOF_DATASET}.ta_bpr_instance`;

    INSERT INTO `{GCP_PROJECT}.{SOF_DATASET}.ta_bpr_instance`
    (
      CNTRCT_ID,
      BPR_ID,
      BPR_INSTANCE_ID,
      ICCID,
      IMSI_MCC,
      IMSI_MNC,
      IMSI_HLR,
      IMSI_SI,
      CNTRCT_ID_REF
    )
    SELECT
      bp.cntrct_id,
      bp.bpr_id,
      bp.bpri_com_id AS bpr_instance_id,
      CONCAT(
        COALESCE(bp.iccid_mi, ''), '-',
        COALESCE(bp.iccid_ii, ''), '-',
        COALESCE(bp.iccid_iai, ''), '-',
        COALESCE(bp.iccid_nr, ''), '-',
        COALESCE(bp.iccid_cd, '')
      ) AS iccid,
      bp.imsi_mcc,
      bp.imsi_mnc,
      bp.imsi_hlr,
      bp.imsi_si,
      bp.cntrct_id_ref
    FROM `{GCP_PROJECT}.{CDS_DATASET}.ta_cntrct` c
    JOIN `{GCP_PROJECT}.{PDS_DATASET}.ta_bpri_com` bp
      ON c.cntrct_id = bp.cntrct_id
    WHERE c.cntrct_st IN (5, 6)
      AND c.redundant_owner_id = 1
      AND DATE(c.insert_at) <= v_datum
      AND (c.modified_at IS NULL OR DATE(c.modified_at) > v_datum)
      AND DATE(c.valid_from) <= v_datum
      AND (c.valid_to IS NULL OR DATE(c.valid_to) > v_datum)
      AND c.is_production = 1
      AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
      AND DATE(bp.insert_at) <= v_datum
      AND (bp.modified_at IS NULL OR DATE(bp.modified_at) > v_datum)
      AND DATE(bp.valid_from) <= v_datum
      AND (bp.valid_to IS NULL OR DATE(bp.valid_to) > v_datum)
      AND bp.is_production = 1;
    """

    execute_bpr_instance_job = BigQueryExecuteQueryOperator(
        task_id="run_d_ausd_bp_ta_bpr_instance",
        sql=sql_script,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
        location=LOCATION,
    )

    execute_bpr_instance_job
```

---

## 5. ENVIRONMENT-SPECIFIC VALUES

To guarantee portability across environments (DEV, QA, PROD), all hardcoded environment elements have been externalized.
1.  **Airflow Variables**:
    *   `gcp_project_id`: GCP Project housing target BigQuery datasets (e.g., `gcp-dwh-dev`).
    *   `gcp_location`: Data processing region (e.g., `EU` or `us-east1`).
    *   `isbert_dataset`: Schema containing tracking tables like `dwtk_meldungen` (defaults to `isbert_schema`).
    *   `sof_dataset`: Schema containing the target table `ta_bpr_instance` (defaults to `sof`).
    *   `cds_dataset`: Schema containing `ta_cntrct` (defaults to `cds`).
    *   `pds_dataset`: Schema containing `ta_bpri_com` (defaults to `pds`).
2.  **Connections**:
    *   An Airflow connection ID named `google_cloud_default` must be configured with corresponding IAM permissions to submit BigQuery jobs.

---

## 6. RISKS AND MANUAL STEPS

*   **Remote Table Syncing**: The legacy script uses `@pcrs1` which refers to an Oracle DB Link. Before executing this DAG, ensure that the pipelines replicating `cds$ta_cntrct` and `pds$ta_bpri_com` into GCP are fully synchronized and accessible in BigQuery.
*   **Concatenation on NULLs**: If any of the `iccid_...` columns contain `NULL` values, the BigQuery `CONCAT` function will return `NULL` for the whole string if not handled properly. The production script resolves this using `COALESCE` with empty strings (`''`) to match the legacy Oracle fallback behavior.
*   **Timezone alignment**: Oracle and UC4 run in local server time (typically CET/CEST). BigQuery processes timestamps as UTC. Ensure that insertion dates in `dwtk_meldungen` align with local boundaries or run date converters to avoid date mismatch filters.