# MIGRATION DESIGN DOCUMENT

**Seed Name:** `ausd_bp_ta_bpr_evn`  
**Seed Type:** JOB  
**Source Root:** `/home/gurunathan_t/test_lineage_data`  
**Target Platform:** BigQuery  

---

## 1. Executive Summary
This document defines the migration design for the job `ausd_bp_ta_bpr_evn`. This job was originally assembled from a UC4/Automic XML job definition that triggers a series of KornShell wrappers and ultimately executes an Oracle SQL*Plus script to process and prepare instantiated basic products (specifically Einzelverbindungsnachweis / EVN) for the BERT subsystem.

The legacy components will be unified and migrated to a modern cloud-native architecture on **Google Cloud Platform (GCP)** using **Apache Airflow (Cloud Composer)** for orchestration and **BigQuery** for high-performance data warehousing.

---

## 2. Legacy to Target Architecture Mapping

| Legacy Component | Legacy Tech | Target Component | Target Tech | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Orchestration** | UC4/Automic XML (`DW.BERT_AUSD_BP_TA_BPR_EVN`) | Airflow DAG | Python (`dags/dw_bert_ausd_bp_ta_bpr_evn.py`) | Orchestrates scheduling, parameter retrieval (Stichtag), and execution tasks. |
| **Wrapper Script** | KornShell (`r_ausd_bp_ta_bpr_evn.ksh`) | DAG Tasks / Config | Python / Airflow Operators | Manages job initializations, parameter parsing, and logging. |
| **Control Script** | KornShell (`k_ausd_bp_ta_bpr_evn.ksh`) | DAG Logic / BigQuery | BigQuery SQL / Airflow Operators | Prepares execution contexts and runs the SQL workload. Commented-out legacy files processing is retired. |
| **Database Processing** | Oracle SQL*Plus (`d_ausd_bp_ta_bpr_evn.sql`) | BigQuery Query | BigQuery SQL (`dags/sql/d_ausd_bp_ta_bpr_evn.sql`) | Performs table truncation and populates target tables using BigQuery standard SQL. |

---

## 3. Data Lineage & Cross-File Dependencies

### 3.1 Lineage Edges
* **Upstream Data Sources:**
  * `sof$ta_bpr_instance` (Read): Oracle source table containing all basis product instances.
  * `isbert_schema.dwtk_meldungen` (Read): Contains metadata and drop triggers.
* **Downstream Target:**
  * `sof$ta_bpr_evn` (Write): Oracle target table containing filtered EVN-specific product instances.
* **Execution Sequence:**
  1. **Airflow Task `parse_parameters`**: Computes or parses `stichtag` (reference date) and `wiederanlaufwert` (restart threshold).
  2. **Airflow Task `truncate_target`**: Truncates target BigQuery table ``sof$ta_bpr_evn``.
  3. **Airflow Task `insert_evn_data`**: Filters and inserts records from ``sof$ta_bpr_instance`` into ``sof$ta_bpr_evn``.

### 3.2 Core Architectural Finding: Commented-Out Legacy Logic
In the legacy control script `k_ausd_bp_ta_bpr_evn.ksh`, there is a large block of code involving temporary file manipulation (`sed`, `sort`, `join`) of local `.dat` and `.csv` files (`cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`) to build a combined `cibasisprodukt.csv`.
* **Current Legacy Status:** This code is completely **commented out** in production.
* **Migration Strategy:** By default, these commented-out operations are **retired** and not active in the core migration path. However, should downstream requirements ever mandate their reactivation, the Python and BigQuery models generated during migration (see MCP output section) provide a direct SQL-based implementation that joins equivalent BigQuery tables (`cibasis_data24`, `cibasis_data96`, `cibasis_fax`) inside BigQuery, bypassing local file systems entirely.

---

## 4. Target File Plan

The migrated code will be organized in a repository structured for deployment to Google Cloud Composer:

| Target File Path | Target Language | Purpose | Legacy Source File |
| :--- | :--- | :--- | :--- |
| `dags/dw_bert_ausd_bp_ta_bpr_evn.py` | Python (Airflow 2.x) | Core Airflow DAG orchestrating the execution sequence and parameter handling. | `DW.BERT_AUSD_BP_TA_BPR_EVN.xml`, `r_ausd_bp_ta_bpr_evn.ksh`, `k_ausd_bp_ta_bpr_evn.ksh` |
| `dags/sql/d_ausd_bp_ta_bpr_evn.sql` | BigQuery SQL (BQSQL) | Clears target table and performs insertion of EVN data. | `d_ausd_bp_ta_bpr_evn.sql` |

---

## 5. Environment-Specific Configurations

To ensure a smooth promotion across environments (Development, Test, Production), use the following configuration mappings:

* **BigQuery Project & Dataset Naming:**
  * Oracle schemas contain special characters like `$`. Since BigQuery datasets and tables support standard alphanumeric characters and underscores, it is highly recommended to perform schema-to-dataset and table-to-table mappings:
    * Oracle Schema `isbert_schema` $\rightarrow$ BigQuery Dataset `${GCP_PROJECT}.isbert_schema`
    * Oracle Table `sof$ta_bpr_instance` $\rightarrow$ BigQuery Table ``${GCP_PROJECT}.isbert_schema.sof_ta_bpr_instance`` or wrap in backticks: ```${GCP_PROJECT}.isbert_schema.sof$ta_bpr_instance```. To maximize compatibility across BigQuery client drivers, **converting `$` to `_`** is the recommended best practice.
* **Airflow Connections & Variables:**
  * `gcp_conn_id`: Airflow connection ID representing GCP credentials (`google_cloud_default`).
  * `gcp_location`: Dataset location (e.g., `EU` or `US`).
  * `stichtag`: Passed dynamically via DAG Run configuration: `{{ dag_run.conf.get('stichtag', ds_nodash) }}`.

---

## 6. Risks, Mitigation, & Manual Steps

1. **Character Replacements in Identifiers:** BigQuery supports standard identifiers. Ensure that downstream applications querying `sof$ta_bpr_evn` or `sof$ta_bpr_instance` are updated if the dollar sign `$` is replaced with an underscore `_`.
2. **Oracle Hints and Session Parameters:**
   * The hint `/*+ full(bp) parallel(bp,4) */` present in the legacy SQL is Oracle-specific. This is safely ignored in BigQuery, as BigQuery is a serverless, columnar execution engine that optimizes parallel scans automatically. No manual performance tuning of degree of parallelism is necessary.
3. **Date Formats:** The legacy system relies on dates formatted as `'DDMMYYYY'` or `'YYYYMMDD'`. BigQuery prefers standard `DATE` types (`YYYY-MM-DD`). Ensure date ingestion pipelines convert legacy date strings into proper BigQuery `DATE` or `TIMESTAMP` objects.

---

## 7. VERBATIM MCP TOOL OUTPUT

Below is the complete and unaltered output of the code transformation tools for each of the four component files.

### === Result for vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BPR_EVN.xml ===
```python
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

try:
    from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
except Exception:
    from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def build_ausd_bp_ta_bpr_evn_sql():
    # Single SQL statement encapsulating the full BigQuery processing logic.
    # Replace the placeholder SQL below with the complete transformation logic
    # required for the BERT_P_BASISPRODUKT preparation workflow.
    sql = """
    -- Create target table if it does not exist and process the data in one query.
    CREATE TABLE IF NOT EXISTS `project.dataset.target_table` AS
    SELECT
        *
    FROM
        `project.dataset.source_table`
    WHERE
        1 = 0;

    INSERT INTO `project.dataset.target_table`
    SELECT
        src.*
    FROM
        `project.dataset.source_table` AS src
    WHERE
        src.process_flag = TRUE;
    """
    return sql


with DAG(
    dag_id="DW_BERT_AUSD_BP_TA_BPR_EVN",
    default_args=default_args,
    description="BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "bert", "basisprodukt"],
) as dag:

    # Single BigQuery operator executing the complete SQL logic.
    process_basisprodukte = BigQueryExecuteQueryOperator(
        task_id="process_basisprodukte",
        sql=build_ausd_bp_ta_bpr_evn_sql(),
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        gcp_conn_id="google_cloud_default",
        location="EU",
    )

    # Task dependency placeholder for extensibility.
    process_basisprodukte
```

### === Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_evn.ksh ===
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
with DAG(
    dag_id="d_ausd_bp_ta_bpr_evn_bigquery",
    default_args=default_args,
    description="BigQuery data processing DAG for PoolBasisprodukt",
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "poolbasisprodukt", "data_processing"],
) as dag:

    # Python function to encapsulate all SQL logic in a single place
    def build_bigquery_sql(p_stichtag: str, p_jobkennung: str, p_eintragsnr: str, p_wiederanlaufwert: str = "0") -> str:
        # Single SQL statement block for all processing logic
        sql = f"""
        -- BigQuery processing logic for PoolBasisprodukt
        -- Parameters:
        --   p_stichtag: {p_stichtag}
        --   p_jobkennung: {p_jobkennung}
        --   p_eintragsnr: {p_eintragsnr}
        --   p_wiederanlaufwert: {p_wiederanlaufwert}

        CREATE TABLE IF NOT EXISTS `your_project.your_dataset.PoolBasisprodukt`
        PARTITION BY DATE(_PARTITIONTIME)
        AS
        WITH
        -- Source data preparation
        data24 AS (
            SELECT
                CAST(col1 AS STRING) AS key_id,
                CAST(col2 AS STRING) AS value_24
            FROM `your_project.your_dataset.cibasis_data24`
            WHERE stichtag = '{p_stichtag}'
        ),
        data96 AS (
            SELECT
                CAST(col1 AS STRING) AS key_id,
                CAST(col2 AS STRING) AS value_96
            FROM `your_project.your_dataset.cibasis_data96`
            WHERE stichtag = '{p_stichtag}'
        ),
        fax AS (
            SELECT
                CAST(col1 AS STRING) AS key_id,
                CAST(col2 AS STRING) AS value_fax
            FROM `your_project.your_dataset.cibasis_fax`
            WHERE stichtag = '{p_stichtag}'
        ),
        -- Combine 24 and 96 datasets
        joined_24_96 AS (
            SELECT
                COALESCE(d24.key_id, d96.key_id) AS key_id,
                d24.value_24,
                d96.value_96
            FROM data24 d24
            FULL OUTER JOIN data96 d96
            ON d24.key_id = d96.key_id
        ),
        -- Final join with fax dataset
        final_result AS (
            SELECT
                COALESCE(j.key_id, f.key_id) AS key_id,
                j.value_24,
                j.value_96,
                f.value_fax,
                '{p_stichtag}' AS stichtag,
                '{p_jobkennung}' AS jobkennung,
                '{p_eintragsnr}' AS eintragsnr,
                '{p_wiederanlaufwert}' AS wiederanlaufwert,
                CURRENT_TIMESTAMP() AS load_ts
            FROM joined_24_96 j
            FULL OUTER JOIN fax f
            ON j.key_id = f.key_id
        )
        SELECT
            key_id,
            value_24,
            value_96,
            value_fax,
            stichtag,
            jobkennung,
            eintragsnr,
            wiederanlaufwert,
            load_ts
        FROM final_result;
        """
        return sql

    # Python callable to prepare SQL text
    def prepare_sql(**context):
        # Pull runtime parameters from DAG run conf or use defaults
        dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}
        p_stichtag = dag_run_conf.get("p_stichtag", "01012000")
        p_jobkennung = dag_run_conf.get("p_jobkennung", "UNKNOWN_JOB")
        p_eintragsnr = dag_run_conf.get("p_eintragsnr", "0")
        p_wiederanlaufwert = dag_run_conf.get("p_wiederanlaufwert", "0")
        return build_bigquery_sql(
            p_stichtag=p_stichtag,
            p_jobkennung=p_jobkennung,
            p_eintragsnr=p_eintragsnr,
            p_wiederanlaufwert=p_wiederanlaufwert,
        )

    # Task to build SQL in a modular way
    build_sql_task = PythonOperator(
        task_id="build_sql",
        python_callable=prepare_sql,
        provide_context=True,
    )

    # Single BigQuery operator executing the complete SQL logic
    execute_bigquery_task = BigQueryExecuteQueryOperator(
        task_id="execute_poolbasisprodukt_processing",
        sql="{{ ti.xcom_pull(task_ids='build_sql') }}",
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        destination_dataset_table="your_project.your_dataset.PoolBasisprodukt",
        gcp_conn_id="google_cloud_default",
    )

    # Task dependencies
    build_sql_task >> execute_bigquery_task
```

### === Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh ===
```python
from datetime import timedelta
from airflow import DAG
from airflow.operators.bigquery_operator import BigQueryExecuteQueryOperator
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

dag = DAG(
    dag_id="bereitstellung_basisprodukte_bert",
    default_args=default_args,
    description="Initiale Bereitstellung ausgewählter Basisprodukte für BERT",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "bert", "basisprodukte"],
)

def build_bq_sql(stichtag="{{ dag_run.conf.get('stichtag', ds_nodash) }}", wiederanlaufwert="{{ dag_run.conf.get('wiederanlaufwert', 0) }}"):
    # SQL logic for the full BigQuery processing in a single query.
    # The query uses a target table that is created if needed and applies
    # the restart threshold to filter out records below the requested ID.
    return f"""
    DECLARE p_stichtag STRING DEFAULT '{stichtag}';
    DECLARE p_wiederanlaufwert INT64 DEFAULT CAST('{wiederanlaufwert}' AS INT64);

    -- Create or replace the target table only if needed by the operator configuration.
    -- The query below performs the complete processing in one statement.
    MERGE `project.dataset.target_table` T
    USING (
      WITH source_data AS (
        SELECT
          *
        FROM `project.dataset.source_table`
        WHERE
          FORMAT_DATE('%d%m%Y', DATE(gueltig_von)) <= p_stichtag
          AND p_stichtag < FORMAT_DATE('%d%m%Y', DATE(gueltig_bis))
          AND FORMAT_DATE('%d%m%Y', DATE(ladedatum)) < p_stichtag
          AND dwh_vertrag_id > p_wiederanlaufwert
      ),
      deduplicated_data AS (
        SELECT
          *
        FROM source_data
      )
      SELECT
        *
      FROM deduplicated_data
    ) S
    ON T.dwh_vertrag_id = S.dwh_vertrag_id
    WHEN MATCHED THEN
      UPDATE SET
        T.col1 = S.col1,
        T.col2 = S.col2,
        T.col3 = S.col3
    WHEN NOT MATCHED THEN
      INSERT (
        dwh_vertrag_id,
        col1,
        col2,
        col3
      )
      VALUES (
        S.dwh_vertrag_id,
        S.col1,
        S.col2,
        S.col3
      );
    """

def process_basisprodukte_bert():
    # Single BigQuery operator encapsulating the complete SQL processing.
    sql_query = build_bq_sql()
    return BigQueryExecuteQueryOperator(
        task_id="bereitstellung_basisprodukte_bert_bq",
        sql=sql_query,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        bigquery_conn_id="google_cloud_default",
        dag=dag,
    )

task = process_basisprodukte_bert()
task
```

### === Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_evn.sql ===
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

# SQL logic encapsulated in a single Python function
def build_bigquery_sql():
    sql = """
    -- Step 1: Clear the target table so the DAG can be safely rerun on the same day.
    TRUNCATE TABLE `sof$ta_bpr_evn`;

    -- Step 2: Insert the EVN basis product instances into the target table.
    INSERT INTO `sof$ta_bpr_evn` (cntrct_id, bpr_id)
    SELECT
        bp.cntrct_id,
        bp.bpr_id
    FROM `sof$ta_bpr_instance` AS bp
    WHERE bp.bpr_id IN (
        32,    -- standard-evn
        2506,  -- komfort-evn
        2839,  -- standard-evn separat
        2840,  -- komfort-evn separat
        3055,  -- komfort-plus-evn
        3056,  -- komfort-plus-evn separat
        3821   -- standard-plus-evn
    );
    """
    return sql

# DAG definition
with DAG(
    dag_id="d_ausd_bp_ta_bpr_evn",
    default_args=default_args,
    description="BigQuery DAG for processing EVN basis product data",
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "basisprodukt", "evn"],
) as dag:

    # Single BigQuery operator executing all SQL logic in one task
    process_evn_basis_products = BigQueryExecuteQueryOperator(
        task_id="process_evn_basis_products",
        sql=build_bigquery_sql(),
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        location="EU",
    )

    # Task dependency placeholder for modular extensibility
    process_evn_basis_products
```