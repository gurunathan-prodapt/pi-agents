# MIGRATION DESIGN DOCUMENT

## 1. Executive Summary & Job Overview
- **Job Name:** `ausd_bp_ta_msisdn_his`
- **Source Technology:** UC4 / Automic, KornShell (KSH), Oracle SQL*Plus
- **Target Platform:** Google Cloud Platform (GCP) - BigQuery & Cloud Composer (Airflow)
- **Job Description:** This job orchestrates and prepares the MSISDN (Mobile Station International Subscriber Directory Number) history data. It truncates the target history table `sof$ta_msisdn_his`, reads from the Carmen database via a database link, and reconstructs the valid subscriber numbers for active products on a specific reporting date (`Stichtag`).

---

## 2. Lineage & Execution Flow
### Legacy Call Chain:
1. **UC4 Job (`DW.BERT_AUSD_BP_TA_MSISDN_HIS.xml`):** Triggers the orchestrator shell script.
2. **Orchestrator Wrapper (`r_ausd_bp_ta_msisdn_his.ksh`):** Parses inputs (Stichtag `-s` and Wiederanlaufwert `-l`), calculates defaults (using system dates), handles error trapping, and executes the control script.
3. **Control Script (`k_ausd_bp_ta_msisdn_his.ksh`):** Performs parameter validation, checks dates using utilities, determines yesterday's date, and starts the SQL*Plus script.
4. **SQL*Plus Script (`d_ausd_bp_ta_msisdn_his.sql`):** 
   - Queries `isbert_schema.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'` to fetch a watermark date (`v_datum`).
   - Truncates `sof$ta_msisdn_his`.
   - Selects from Carmen's `pds$ta_callnumber@pcrs1` (filtering by `v_datum` watermarks and active product flags), concatenates country code (`cc`), network destination code (`ndc`), and subscriber number (`sn`), and inserts them into `sof$ta_msisdn_his`.

### Upstream & Downstream Dependencies:
- **Upstream Producers:** 
  - Carmen source database (`pds.ta_callnumber`) via a database link `@pcrs1`.
  - Logging/Notification process that inserts into `isbert_schema.dwtk_meldungen` under `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
- **Downstream Consumers:** 
  - Forderungsscoring (FOS) loaders and further BERT base product provisioning downstream processes.

---

## 3. External System & Architecture Replacements
- **Database Link (`@pcrs1`):** The Oracle database link pointing to Carmen is decommissioned. In BigQuery, this is replaced by referencing a replicated dataset (e.g., `gcp-project.pds.ta_callnumber`) loaded via a standardized data integration pipeline.
- **Oracle PL/SQL Utilities:** Shell-based utilities (`h_alis_date.ksh`, `h_alis_parameter.ksh`, `f_alis_msgerr.ksh`) and PL/SQL packages like `isbert_schema.DWPA_UTIL_SKRIPT` are replaced by standard GCP Airflow operators and native BigQuery DDL/DML functions.
- **Orchestration Tooling:** UC4 scheduler definitions are converted to a unified Airflow DAG running on Cloud Composer.

---

## 4. Environment-Specific Configurations & Parameter Mapping
- **BigQuery Projects & Datasets:**
  - Staging/Target Schema: `sof` -> `gcp-project-id.sof_dataset`
  - Metadata Schema: `isbert_schema` -> `gcp-project-id.isbert_dataset`
  - Source Schema: `pds` -> `gcp-project-id.pds_dataset`
- **Airflow Variables & Runtime Parameters:**
  - `stichtag`: Passed as DAG execution parameter `{{ dag_run.conf.get('stichtag') }}` or defaulted to `ds` (Airflow execution date).
  - `wiederanlaufwert` (Restart ID): Parsed via DAG configuration parameter `{{ dag_run.conf.get('wiederanlaufwert', 0) }}`.

---

## 5. Target File Plan
The legacy orchestration pipeline is consolidated into the following target structure:

| Legacy Source File | Target Path | Target Language | Description |
| :--- | :--- | :--- | :--- |
| `DW.BERT_AUSD_BP_TA_MSISDN_HIS.xml`<br>`r_ausd_bp_ta_msisdn_his.ksh`<br>`k_ausd_bp_ta_msisdn_his.ksh` | `dags/bereitstellung_basisprodukte_bert.py` | Python (Airflow DAG) | Airflow DAG orchestrating parameter preparation, date determination, and BigQuery job execution. |
| `d_ausd_bp_ta_msisdn_his.sql` | `queries/d_ausd_bp_ta_msisdn_his.sql` | BigQuery SQL | SQL script that performs the TRUNCATE and INSERT-SELECT mapping for MSISDN history. |

---

## 6. VERBATIM MCP TOOL OUTPUTS
Below is the exact output from the CodeMaverick design generation tool, generated individually for each source file:

### === Result for vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_MSISDN_HIS.xml ===
```python
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

try:
    from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
except ImportError:
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


def build_ausd_bp_ta_msisdn_his_sql():
    # Single SQL statement encapsulating the full BigQuery processing logic.
    # Replace the placeholder SQL below with the complete transformation logic
    # required for DW.BERT_AUSD_BP_TA_MSISDN_HIS.
    sql = """
    CREATE OR REPLACE TABLE `your_project.your_dataset.DW_BERT_AUSD_BP_TA_MSISDN_HIS` AS
    SELECT
        *
    FROM `your_project.your_dataset.source_table`
    """
    return sql


with DAG(
    dag_id="DW_BERT_AUSD_BP_TA_MSISDN_HIS",
    default_args=default_args,
    description="BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "dw", "bert", "basisprodukt"],
) as dag:

    # Build the complete SQL in a single Python function.
    query_sql = build_ausd_bp_ta_msisdn_his_sql()

    # Execute the full BigQuery logic in a single operator.
    process_ausd_bp_ta_msisdn_his = BigQueryExecuteQueryOperator(
        task_id="process_ausd_bp_ta_msisdn_his",
        sql=query_sql,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        destination_dataset_table="your_project:your_dataset.DW_BERT_AUSD_BP_TA_MSISDN_HIS",
    )

    # Single-task DAG; explicit dependency kept for clarity and extensibility.
    process_ausd_bp_ta_msisdn_his
```

### === Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn_his.ksh ===
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
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="d_ausd_bp_ta_msisdn_his_bigquery",
    default_args=default_args,
    description="BigQuery processing DAG for PoolBasisprodukt based on legacy shell script logic",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["bigquery", "poolbasisprodukt", "legacy-migration"],
) as dag:

    def build_bigquery_sql(p_stichtag: str, p_jobkennung: str, p_eintragsnr: str, p_wiederanlaufwert: str = "0") -> str:
        # Single SQL statement encapsulating the full processing logic.
        # Target table is created if it does not exist via CREATE_IF_NEEDED.
        sql = f"""
        CREATE TABLE IF NOT EXISTS `your_project.your_dataset.PoolBasisprodukt`
        PARTITION BY DATE(_PARTITIONTIME)
        AS
        WITH
        params AS (
            SELECT
                '{p_stichtag}' AS stichtag,
                '{p_jobkennung}' AS jobkennung,
                '{p_eintragsnr}' AS eintragsnr,
                '{p_wiederanlaufwert}' AS wiederanlaufwert
        ),
        source_data AS (
            SELECT
                *
            FROM `your_project.your_dataset.source_table`
            WHERE TRUE
        ),
        transformed_data AS (
            SELECT
                sd.*,
                p.stichtag,
                p.jobkennung,
                p.eintragsnr,
                p.wiederanlaufwert
            FROM source_data sd
            CROSS JOIN params p
        ),
        final_data AS (
            SELECT
                *
            FROM transformed_data
        )
        SELECT
            *
        FROM final_data
        """
        return sql

    def create_bigquery_task():
        # Build the SQL once and execute it with a single BigQuery operator.
        query = build_bigquery_sql(
            p_stichtag="{{ dag_run.conf.get('p_Stichtag', '') }}",
            p_jobkennung="{{ dag_run.conf.get('p_JobKennung', '') }}",
            p_eintragsnr="{{ dag_run.conf.get('p_EintragsNr', '') }}",
            p_wiederanlaufwert="{{ dag_run.conf.get('p_wiederanlaufWert', '0') }}",
        )

        return BigQueryExecuteQueryOperator(
            task_id="process_poolbasisprodukt",
            sql=query,
            use_legacy_sql=False,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            location="EU",
            gcp_conn_id="google_cloud_default",
        )

    # Single task for the complete BigQuery processing
    process_poolbasisprodukt = create_bigquery_task()

    # Task dependencies
    process_poolbasisprodukt
```

### === Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh ===
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id="bereitstellung_basisprodukte_bert",
    default_args=default_args,
    description="Initial provision of selected base products for BERT using BigQuery",
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "bert", "basisprodukte"],
)

def build_bigquery_sql(stichtag=None, wiederanlaufwert=0):
    """
    Build the full BigQuery SQL statement for the BERT base product provisioning.
    The SQL is kept in a single query and can create the target table if needed.
    """
    stichtag_filter = f"DATE('{stichtag}')" if stichtag else "CURRENT_DATE()"
    wiederanlaufwert = int(wiederanlaufwert) if wiederanlaufwert is not None else 0

    sql = f"""
    -- Create or replace the target table content in a single BigQuery statement
    -- The target table is created automatically if it does not exist.
    CREATE TABLE IF NOT EXISTS `your_project.your_dataset.bert_basisprodukte_target` AS
    WITH source_data AS (
        SELECT
            *
        FROM `your_project.your_dataset.contract_cache_source`
        WHERE
            DATE(gueltig_von) <= {stichtag_filter}
            AND {stichtag_filter} < DATE(gueltig_bis)
            AND DATE(ladedatum) < {stichtag_filter}
            AND dwh_vertrag_id > {wiederanlaufwert}
    ),
    filtered_data AS (
        SELECT
            *
        FROM source_data
        WHERE dwh_vertrag_id > {wiederanlaufwert}
    )
    SELECT
        *
    FROM filtered_data
    """
    return sql

def create_bq_task(**context):
    """
    Create the BigQuery task that executes the full SQL logic in one operator.
    """
    stichtag = context["dag_run"].conf.get("stichtag") if context.get("dag_run") and context["dag_run"].conf else None
    wiederanlaufwert = context["dag_run"].conf.get("wiederanlaufwert", 0) if context.get("dag_run") and context["dag_run"].conf else 0

    query = build_bigquery_sql(stichtag=stichtag, wiederanlaufwert=wiederanlaufwert)

    return BigQueryExecuteQueryOperator(
        task_id="bereitstellung_basisprodukte_bert_bq",
        sql=query,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud_default",
        dag=dag,
    )

# Optional Python task to prepare or validate runtime parameters
def prepare_parameters(**context):
    """
    Prepare runtime parameters such as stichtag and wiederanlaufwert.
    """
    dag_run_conf = context["dag_run"].conf if context.get("dag_run") and context["dag_run"].conf else {}
    context["ti"].xcom_push(key="stichtag", value=dag_run_conf.get("stichtag"))
    context["ti"].xcom_push(key="wiederanlaufwert", value=dag_run_conf.get("wiederanlaufwert", 0))

prepare_params = PythonOperator(
    task_id="prepare_parameters",
    python_callable=prepare_parameters,
    provide_context=True,
    dag=dag,
)

def execute_bigquery(**context):
    """
    Instantiate and execute the single BigQuery operator with the full SQL logic.
    """
    stichtag = context["ti"].xcom_pull(task_ids="prepare_parameters", key="stichtag")
    wiederanlaufwert = context["ti"].xcom_pull(task_ids="prepare_parameters", key="wiederanlaufwert")

    bq_task = BigQueryExecuteQueryOperator(
        task_id="bereitstellung_basisprodukte_bert_bq",
        sql=build_bigquery_sql(stichtag=stichtag, wiederanlaufwert=wiederanlaufwert),
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud_default",
        dag=dag,
    )
    return bq_task.execute(context=context)

run_bq = PythonOperator(
    task_id="run_bq",
    python_callable=execute_bigquery,
    provide_context=True,
    dag=dag,
)

# Task dependencies
prepare_params >> run_bq
```

### === Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_msisdn_his.sql ===
```python
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

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
    # Truncate target table to support reruns on the same day
    truncate_sql = """
    TRUNCATE TABLE `sof.ta_msisdn_his`
    """

    # Insert valid MSISDN records into the target table
    insert_sql = """
    INSERT INTO `sof.ta_msisdn_his`
        (
            BPRI_COM_ID,
            MSISDN,
            CALLNUMBER_ROLE_ID,
            VALID_TO
        )
    SELECT
        cn1.bpri_com_id AS BPRI_COM_ID,
        CONCAT(cn1.cc, cn1.ndc, cn1.sn) AS MSISDN,
        cn1.callnumber_role_id AS CALLNUMBER_ROLE_ID,
        cn1.valid_to AS VALID_TO
    FROM `pds.ta_callnumber` cn1
    WHERE cn1.insert_at <= DATE(
            COALESCE(
                (
                    SELECT MAX(DATE(m.timecreated))
                    FROM `isbert_schema.dwtk_meldungen` m
                    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
                ),
                DATE '1900-01-01'
            )
        )
      AND (
            cn1.modified_at IS NULL
            OR cn1.modified_at > DATE(
                COALESCE(
                    (
                        SELECT MAX(DATE(m.timecreated))
                        FROM `isbert_schema.dwtk_meldungen` m
                        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
                    ),
                    DATE '1900-01-01'
                )
            )
          )
      AND cn1.valid_from <= DATE(
            COALESCE(
                (
                    SELECT MAX(DATE(m.timecreated))
                    FROM `isbert_schema.dwtk_meldungen` m
                    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
                ),
                DATE '1900-01-01'
            )
        )
      AND cn1.is_production = 1
    """

    # Combine all SQL logic into a single script
    return f"""
    {truncate_sql};

    {insert_sql};
    """

# DAG definition
with DAG(
    dag_id="d_ausd_bp_msisdn_his",
    default_args=default_args,
    description="Process MSISDN history data into BigQuery target table",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "msisdn", "history"],
) as dag:

    # Single BigQuery operator executing the full SQL logic
    process_msisdn_history = BigQueryExecuteQueryOperator(
        task_id="process_msisdn_history",
        sql=build_bigquery_sql(),
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
    )

    # Task dependency placeholder for extensibility
    process_msisdn_history
```

---

## 7. Risks, Validation, and Manual Steps
- **Concatenation and Null Safety:** The original Oracle SQL performs `cn1.cc||cn1.ndc||cn1.sn`. In Oracle, concatenation of NULL values yields the remaining string. In BigQuery, `CONCAT(...)` returns `NULL` if any argument is `NULL`. 
  - *Recommendation:* Replace the naive `CONCAT` with `CONCAT(COALESCE(cn1.cc, ''), COALESCE(cn1.ndc, ''), COALESCE(cn1.sn, ''))` in the production-ready code.
- **Dynamic Watermark Timing:** The watermark query checks `isbert_schema.dwtk_meldungen` for the max date associated with the job identifier `BERT_DROP_TEMP_TABLE`. Ensure that this upstream job runs and commits to the database prior to invoking `ausd_bp_ta_msisdn_his`.
- **Date String Formats:** The legacy shell script checks dates against format `DDMMYYYY`. Ensure parameter conversion in Airflow accurately parses incoming configuration values and normalizes them to ISO format (`YYYY-MM-DD`) for native BigQuery operations.