# MIGRATION DESIGN DOCUMENT: `ausd_bp_ta_apn_vertrag`

This Migration Design Document outlines the transition from a legacy Oracle-based scheduling and ETL architecture to Google Cloud Platform (GCP) BigQuery and Apache Airflow (Cloud Composer).

---

## 1. Executive Summary & Migration Strategy

The job `ausd_bp_ta_apn_vertrag` is a core batch process within the BERT/DWH system. Its primary purpose is to aggregate active basic products (e.g., Access Point Names (APN) and contract reference values) from the table `sof$ta_bpr_apn` and format them into concatenated, comma-separated fields per contract (`cntrct_id`) in the target table `sof$ta_apn_vertrag`.

### Legacy Technology Stack
* **Scheduler**: UC4/Automic Job (XML definition calling `r_ausd_bp_ta_apn_vertrag.ksh`)
* **Orchestration**: KornShell (KSH) scripts `r_ausd_bp_ta_apn_vertrag.ksh` and `k_ausd_bp_ta_apn_vertrag.ksh` for parameter validation, environment initialisation, and error logging
* **Database & Transformation**: Oracle PL/SQL block utilizing an explicit cursor loop to concatenate string records, hosted in Oracle SQL*Plus

### Target Technology Stack
* **Orchestration & Workflow**: Cloud Composer (Apache Airflow) DAG executing BigQuery operators
* **Data Warehouse**: Google Cloud BigQuery
* **Transformation Language**: BigQuery SQL (using high-performance standard SQL aggregations to replace the iterative cursor-loop logic)

---

## 2. System Lineage & Architecture

### Upstream and Downstream Lineage
* **Upstream Producer**: The source table `sof$ta_bpr_apn` (represented in BigQuery as `sof_ta_bpr_apn`) must be populated prior to this job. 
* **Control/Log Table**: Oracle table `isbert_schema.dwtk_meldungen` (BigQuery `isbert_schema.dwtk_meldungen`) is queried to resolve variables.
* **Downstream Consumer**: The target table `sof$ta_apn_vertrag` (BigQuery `sof_ta_apn_vertrag`) is consumed by downstream scoring and assessment engines (FOS - Forderungsscoring).

```
   [sof_ta_bpr_apn] --(Source Table)--> [BigQuery Transformation]
                                                   |
[dwtk_meldungen] --(Metadata/Control)-->           v
                                         [sof_ta_apn_vertrag] --(Target)--> [FOS Scoring Engine]
```

### External System Replacements
* **UC4 Jobs & Unix Host**: Replaced by an Airflow DAG. System logins like `DW.UNIX.ISBERT` and connections to host `DWHDWH2P` are mapped to GCP service accounts and IAM roles running Airflow operators.
* **Legacy Error Logging (`DWMSG` framework)**: Ported to standard Airflow task logging and GCS/Cloud Logging.
* **File Handling (Commented out block)**: The legacy KSH file has historical, commented-out sections references to files (`cibasis_data24.dat`, etc.) and command-line processing (sed, sort, join). These are retired and will not be migrated.

### Execution Order & Call Chain
1. **Airflow DAG Trigger**: Scheduled daily or triggered on demand with optional parameters `stichtag` and `wiederanlaufwert`.
2. **Dynamic Date Logic**: Airflow determines the target `Stichtag` parameter (defaulting to the current execution date if not explicitly supplied).
3. **Core Transformation**: The `BigQueryExecuteQueryOperator` runs the SQL transformation, performing a fast, parallelized set-based `STRING_AGG` execution.
4. **Validation and Commit**: The query automatically commits the truncated and replaced rows in the target table.

---

## 3. Target File Plan

Below is the directory plan for the target Airflow and SQL structures. 

| Legacy File Path | Target Relative Path | Target Language | Description |
| :--- | :--- | :--- | :--- |
| `DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml`<br>`r_ausd_bp_ta_apn_vertrag.ksh`<br>`k_ausd_bp_ta_apn_vertrag.ksh` | `dags/dw_bert_ausd_bp_ta_apn_vertrag.py` | Python (Airflow DAG) | Consolidates UC4 scheduling, parameters initialization, and process flow control. |
| `d_ausd_bp_ta_apn_vertrag.sql` | `gcp/bigquery/sql/d_ausd_bp_ta_apn_vertrag.sql` | BigQuery Standard SQL | Replaces Oracle SQL*Plus script and PL/SQL cursor loop with standard BigQuery aggregation. |

---

## 4. Environment-Specific Configurations & Parameters

The target environment requires variables to be configured in Apache Airflow. 

| Airflow Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `gcp_project_id` | `gcp-dwh-prod` | Target Google Cloud Project ID |
| `bq_dataset` | `isbert_schema` | Target BigQuery Dataset hosting tables |
| `bq_location` | `EU` | BigQuery Data Location |
| `gcp_conn_id` | `google_cloud_default` | Airflow Connection ID for GCP access |

---

## 5. Source-to-Target Mappings

### Table & Field-level Mapping

**Source Table**: `sof$ta_bpr_apn` (Oracle)  
**Target Table**: `sof_ta_bpr_apn` (BigQuery)

| Legacy Field | Target Field | Data Type (Legacy) | Data Type (BigQuery) | Rules / Logic |
| :--- | :--- | :--- | :--- | :--- |
| `cntrct_id` | `cntrct_id` | VARCHAR2(10) | STRING | Primary grouping key. |
| `cntrct_id_ref` | `cntrct_id_ref` | VARCHAR2(100) | STRING | Used in concatenation list. |
| `bpr_id` | `bpr_id` | NUMBER | INT64 | Core ID. |
| `access_point_name` | `access_point_name`| VARCHAR2(100) | STRING | Used in concatenation list. |

**Source Table**: `sof$ta_apn_vertrag` (Oracle)  
**Target Table**: `sof_ta_apn_vertrag` (BigQuery)

| Legacy Field | Target Field | Data Type (Legacy) | Data Type (BigQuery) | Rules / Logic |
| :--- | :--- | :--- | :--- | :--- |
| `cntrct_id` | `cntrct_id` | VARCHAR2(10) | STRING | Truncated value of `cntrct_id`. |
| `apn` | `access_point_names` | VARCHAR2(100) | STRING | Aggregated list of APNs up to 100 characters. |
| `cntrct_id_ref` | `cntrct_id_refs` | VARCHAR2(100) | STRING | Aggregated list of references up to 100 characters. |

---

## 6. Risks, Manual Steps, and B4 Redesign Items

1. **Truncation Behavior Difference**: 
   * **Legacy PL/SQL Logic**: Loops over records, check length of concatenated string, and skips the append if it exceeds 100 characters. This means later shorter elements could theoretically be appended while intermediate longer ones are skipped.
   * **BigQuery SQL Logic**: Standardizes this by combining elements with `STRING_AGG` (safely preserving global ordering via `ORDER BY`) and then truncating the resulting string with `SUBSTR(..., 1, 100)`. 
   * **Action**: Ensure business stakeholders approve this consistent, standard truncation approach. If exact emulation of the skipping logic is required, a User Defined Function (UDF) is recommended.
2. **Historical Log Clean-up**:
   * The query checking `isbert_schema.dwtk_meldungen` for job status `BERT_DROP_TEMP_TABLE` must be pointing to the corresponding transformed logging infrastructure in BigQuery. Ensure logging records are written as expected during the run.

---

## 7. Verbatim MCP Output

The following code sections are the exact generated code snippets from the Migration Control Portal for each components.

### === Result for vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml ===
```python
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default DAG arguments
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
    dag_id="dw_bert_ausd_bp_ta_apn_vertrag",
    default_args=default_args,
    description="BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "dw", "bert"],
)

def process_ausd_bp_ta_apn_vertrag():
    # Single SQL statement encapsulating the full processing logic.
    # Replace the SQL body below with the complete transformation logic required
    # for the target BigQuery table.
    sql_query = """
    CREATE OR REPLACE TABLE `your_project.your_dataset.ausd_bp_ta_apn_vertrag` AS
    SELECT
        *
    FROM
        `your_project.your_dataset.source_table`
    WHERE
        1 = 1
    """

    # Single BigQuery operator executing the full SQL logic.
    return BigQueryExecuteQueryOperator(
        task_id="process_ausd_bp_ta_apn_vertrag",
        sql=sql_query,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
    )

# Task instantiation
process_task = process_ausd_bp_ta_apn_vertrag()

# Task dependencies
process_task
```

### === Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh ===
```python
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
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
    description="Initiale Bereitstellung ausgewählter Basisprodukte für BERT",
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "bert", "basisprodukte"],
)

def build_bigquery_sql(stichtag="{{ dag_run.conf.get('stichtag', '') }}", wiederanlaufwert="{{ dag_run.conf.get('wiederanlaufwert', 0) }}"):
    # SQL logic for preparing and loading the target table in a single BigQuery statement.
    # The target table is created if it does not exist via CREATE_IF_NEEDED.
    return f"""
    CREATE TABLE IF NOT EXISTS `{{{{ var.value.gcp_project_id }}}}.{{{{ var.value.bq_dataset }}}}.ausd_bp_ta_apn_vertrag`
    PARTITION BY DATE(gueltig_von)
    CLUSTER BY dwh_vertrag_id
    AS
    WITH
      params AS (
        SELECT
          SAFE_CAST(NULLIF('{stichtag}', '') AS STRING) AS stichtag_raw,
          SAFE_CAST('{wiederanlaufwert}' AS INT64) AS wiederanlaufwert
      ),
      normalized_params AS (
        SELECT
          CASE
            WHEN stichtag_raw IS NULL OR stichtag_raw = '' THEN FORMAT_DATE('%d%m%Y', CURRENT_DATE())
            ELSE stichtag_raw
          END AS stichtag_ddmmyyyy,
          wiederanlaufwert
        FROM params
      ),
      parsed_params AS (
        SELECT
          PARSE_DATE('%d%m%Y', stichtag_ddmmyyyy) AS stichtag,
          wiederanlaufwert
        FROM normalized_params
      ),
      source_contract_cache AS (
        SELECT
          dwh_vertrag_id,
          vertrag_id,
          gueltig_von,
          gueltig_bis,
          ladedatum,
          produkt_code,
          produkt_name,
          vertragsstatus,
          kunden_id,
          scoring_relevant_flag,
          last_update_ts
        FROM `{{{{ var.value.gcp_project_id }}}}.{{{{ var.value.bq_dataset }}}}.ta_vertrag_cache`
      ),
      filtered_contracts AS (
        SELECT
          s.*
        FROM source_contract_cache s
        CROSS JOIN parsed_params p
        WHERE s.gueltig_von <= p.stichtag
          AND p.stichtag < s.gueltig_bis
          AND s.ladedatum < p.stichtag
          AND s.dwh_vertrag_id > p.wiederanlaufwert
      ),
      deduplicated_contracts AS (
        SELECT
          *
        FROM filtered_contracts
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY dwh_vertrag_id
          ORDER BY last_update_ts DESC, ladedatum DESC
        ) = 1
      ),
      deleted_range AS (
        SELECT
          p.wiederanlaufwert AS delete_from_id
        FROM parsed_params p
      ),
      cleaned_target AS (
        SELECT
          t.*
        FROM `{{{{ var.value.gcp_project_id }}}}.{{{{ var.value.bq_dataset }}}}.ausd_bp_ta_apn_vertrag` t
        CROSS JOIN deleted_range d
        WHERE t.dwh_vertrag_id < d.delete_from_id
      )
    SELECT
      dwh_vertrag_id,
      vertrag_id,
      gueltig_von,
      gueltig_bis,
      ladedatum,
      produkt_code,
      produkt_name,
      vertragsstatus,
      kunden_id,
      scoring_relevant_flag,
      last_update_ts
    FROM cleaned_target

    UNION ALL

    SELECT
      dwh_vertrag_id,
      vertrag_id,
      gueltig_von,
      gueltig_bis,
      ladedatum,
      produkt_code,
      produkt_name,
      vertragsstatus,
      kunden_id,
      scoring_relevant_flag,
      last_update_ts
    FROM deduplicated_contracts
    """

def create_processing_task():
    # Single BigQuery operator executing the complete SQL logic.
    sql_query = build_bigquery_sql()
    return BigQueryExecuteQueryOperator(
        task_id="bereitstellung_basisprodukte_bert_bq",
        sql=sql_query,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        location="{{ var.value.bq_location }}",
        dag=dag,
    )

# Optional start/end markers for clear DAG structure
start = BashOperator(
    task_id="start",
    bash_command="echo 'Start bereitstellung_basisprodukte_bert'",
    dag=dag,
)

process = create_processing_task()

end = BashOperator(
    task_id="end",
    bash_command="echo 'End bereitstellung_basisprodukte_bert'",
    dag=dag,
)

# Task dependencies
start >> process >> end
```

### === Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh ===
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


# SQL logic encapsulated in a single Python function
def build_poolbasisprodukt_sql(stichtag: str, job_kennung: str, eintrags_nr: str, wiederanlauf_wert: int = 0) -> str:
    """
    Build the full BigQuery SQL statement for PoolBasisprodukt processing.
    The SQL is kept in a single query block to satisfy the single-operator requirement.
    """
    sql = f"""
    -- BigQuery processing for PoolBasisprodukt
    -- Parameters:
    --   stichtag           : {stichtag}
    --   job_kennung         : {job_kennung}
    --   eintrags_nr         : {eintrags_nr}
    --   wiederanlauf_wert   : {wiederanlauf_wert}

    CREATE TABLE IF NOT EXISTS `your_project.your_dataset.PoolBasisprodukt`
    AS
    WITH
      -- Source data for 24-hour records
      data24 AS (
        SELECT
          CAST(NULL AS STRING) AS key_id,
          CAST(NULL AS STRING) AS value_24
        WHERE FALSE
      ),

      -- Source data for 96-hour records
      data96 AS (
        SELECT
          CAST(NULL AS STRING) AS key_id,
          CAST(NULL AS STRING) AS value_96
        WHERE FALSE
      ),

      -- Source data for fax records
      fax AS (
        SELECT
          CAST(NULL AS STRING) AS key_id,
          CAST(NULL AS STRING) AS value_fax
        WHERE FALSE
      ),

      -- Combine 24h and 96h data
      joined_24_96 AS (
        SELECT
          COALESCE(d96.key_id, d24.key_id) AS key_id,
          d24.value_24,
          d96.value_96
        FROM data24 d24
        FULL OUTER JOIN data96 d96
          ON d24.key_id = d96.key_id
      ),

      -- Combine the previous result with fax data
      final_result AS (
        SELECT
          COALESCE(j.key_id, f.key_id) AS key_id,
          j.value_24,
          j.value_96,
          f.value_fax
        FROM joined_24_96 j
        FULL OUTER JOIN fax f
          ON j.key_id = f.key_id
      )

    SELECT
      key_id,
      value_24,
      value_96,
      value_fax,
      '{stichtag}' AS stichtag,
      '{job_kennung}' AS job_kennung,
      '{eintrags_nr}' AS eintrags_nr,
      {wiederanlauf_wert} AS wiederanlauf_wert
    FROM final_result;
    """
    return sql


# Python callable to prepare SQL text
def prepare_sql(**context):
    # Replace these with Airflow Variables, Params, or XCom values as needed
    stichtag = context["params"].get("stichtag", "01012025")
    job_kennung = context["params"].get("job_kennung", "JOB_DEFAULT")
    eintrags_nr = context["params"].get("eintrags_nr", "1")
    wiederanlauf_wert = int(context["params"].get("wiederanlauf_wert", 0))

    return build_poolbasisprodukt_sql(
        stichtag=stichtag,
        job_kennung=job_kennung,
        eintrags_nr=eintrags_nr,
        wiederanlauf_wert=wiederanlauf_wert,
    )


# DAG definition
with DAG(
    dag_id="d_ausd_bp_ta_apn_vertrag_bigquery",
    default_args=default_args,
    description="BigQuery DAG for PoolBasisprodukt processing",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    params={
        "stichtag": "01012025",
        "job_kennung": "JOB_DEFAULT",
        "eintrags_nr": "1",
        "wiederanlauf_wert": 0,
    },
    tags=["bigquery", "poolbasisprodukt"],
) as dag:

    # Prepare the SQL statement in a single Python function
    prepare_sql_task = PythonOperator(
        task_id="prepare_sql",
        python_callable=prepare_sql,
        provide_context=True,
    )

    # Execute the full SQL in a single BigQuery operator
    run_bigquery_task = BigQueryExecuteQueryOperator(
        task_id="run_poolbasisprodukt_query",
        sql="{{ ti.xcom_pull(task_ids='prepare_sql') }}",
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        location="EU",
        gcp_conn_id="google_cloud_default",
    )

    # Set task dependency
    prepare_sql_task >> run_bigquery_task
```

### === Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql ===
```python
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id='d_ausd_basisprodukt_bigquery',
    default_args=default_args,
    description='BigQuery processing for d_ausd_basisprodukt.sql',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

# Optional start/end markers for traceability
start_task = BashOperator(
    task_id='start_trace',
    bash_command='echo "Starting d_ausd_basisprodukt BigQuery processing"',
    dag=dag,
)

end_task = BashOperator(
    task_id='end_trace',
    bash_command='echo "Finished d_ausd_basisprodukt BigQuery processing"',
    dag=dag,
)

def process_apn_vertrag_sql():
    # Single SQL statement encapsulating the full transformation logic.
    # The target table is created if needed via CREATE_IF_NEEDED.
    return """
    CREATE TABLE IF NOT EXISTS `sof.ta_apn_vertrag` (
      cntrct_id STRING,
      access_point_names STRING,
      cntrct_id_refs STRING
    )
    AS
    WITH ordered_source AS (
      SELECT
        cntrct_id_ref,
        bpr_id,
        cntrct_id,
        access_point_name,
        ROW_NUMBER() OVER (ORDER BY cntrct_id, cntrct_id_ref, access_point_name) AS rn
      FROM `sof.ta_bpr_apn`
    ),
    grouped AS (
      SELECT
        cntrct_id,
        STRING_AGG(access_point_name, ', ' ORDER BY rn) AS access_point_names,
        STRING_AGG(cntrct_id_ref, ', ' ORDER BY rn) AS cntrct_id_refs
      FROM ordered_source
      GROUP BY cntrct_id
    )
    SELECT
      cntrct_id,
      SUBSTR(RTRIM(access_point_names, ', '), 1, 100) AS access_point_names,
      SUBSTR(RTRIM(cntrct_id_refs, ', '), 1, 100) AS cntrct_id_refs
    FROM grouped
    """

# Single BigQuery operator executing the complete SQL logic
process_apn_vertrag_task = BigQueryExecuteQueryOperator(
    task_id='process_apn_vertrag',
    sql=process_apn_vertrag_sql(),
    use_legacy_sql=False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

# Task dependencies
start_task >> process_apn_vertrag_task >> end_task
```