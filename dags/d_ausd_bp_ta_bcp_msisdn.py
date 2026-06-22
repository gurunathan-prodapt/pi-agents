# This Airflow DAG replaces the legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
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
    dag_id="d_ausd_bp_ta_bcp_msisdn",
    default_args=default_args,
    description="BigQuery processing DAG for PoolBasisprodukt based on legacy shell script logic",
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "poolbasisprodukt", "msisdn"],
) as dag:

    def build_bigquery_sql(**context):
        # Retrieve runtime parameters from DAG run configuration or defaults
        dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}

        p_jobkennung = dag_run_conf.get("p_JobKennung", "{{ dag_run.conf.get('p_JobKennung') }}")
        p_eintragsnr = dag_run_conf.get("p_EintragsNr", "{{ dag_run.conf.get('p_EintragsNr') }}")
        p_stichtag = dag_run_conf.get("p_Stichtag", "{{ dag_run.conf.get('p_Stichtag') }}")
        p_wiederanlaufwert = dag_run_conf.get("p_wiederanlaufWert", 0)
        p_datum_heute = dag_run_conf.get("p_datum_heute", "{{ ds }}")
        p_datum_gestern = dag_run_conf.get("p_datum_gestern", "{{ macros.ds_add(ds, -1) }}")

        # Single SQL statement encapsulating the full processing logic
        sql = f"""
        -- Create target table if it does not exist and process data in one BigQuery statement
        CREATE TABLE IF NOT EXISTS `{{{{ var.value.gcp_project }}}}.{{{{ var.value.bq_dataset }}}}.PoolBasisprodukt` (
            job_kennung STRING,
            eintrags_nr STRING,
            stichtag STRING,
            wiederanlauf_wert INT64,
            datum_heute STRING,
            datum_gestern STRING,
            records_processed INT64,
            created_at TIMESTAMP
        );

        INSERT INTO `{{{{ var.value.gcp_project }}}}.{{{{ var.value.bq_dataset }}}}.PoolBasisprodukt`
        (
            job_kennung,
            eintrags_nr,
            stichtag,
            wiederanlauf_wert,
            datum_heute,
            datum_gestern,
            records_processed,
            created_at
        )
        SELECT
            '{p_jobkennung}' AS job_kennung,
            '{p_eintragsnr}' AS eintrags_nr,
            '{p_stichtag}' AS stichtag,
            CAST({int(p_wiederanlaufwert)} AS INT64) AS wiederanlauf_wert,
            '{p_datum_heute}' AS datum_heute,
            '{p_datum_gestern}' AS datum_gestern,
            COUNT(1) AS records_processed,
            CURRENT_TIMESTAMP() AS created_at
        FROM (
            -- Replace this source query with the actual BigQuery transformation logic
            SELECT
                *
            FROM `{{{{ var.value.gcp_project }}}}.{{{{ var.value.source_dataset }}}}.source_table`
            WHERE 1 = 1
              AND DATE(_PARTITIONTIME) BETWEEN DATE_SUB(PARSE_DATE('%d%m%Y', '{p_stichtag}'), INTERVAL 1 DAY)
                                          AND PARSE_DATE('%d%m%Y', '{p_stichtag}')
        );
        """
        return sql

    def create_bigquery_task():
        # Build the SQL dynamically in Python and execute it with a single BigQuery operator
        return BigQueryExecuteQueryOperator(
            task_id="process_poolbasisprodukt",
            sql="{{ ti.xcom_pull(task_ids='build_sql') }}",
            use_legacy_sql=False,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            location="{{ var.value.bq_location }}",
            gcp_conn_id="google_cloud_default",
        )

    # Task to build the SQL statement
    build_sql = PythonOperator(
        task_id="build_sql",
        python_callable=build_bigquery_sql,
        provide_context=True,
    )

    # Single BigQuery execution task
    process_poolbasisprodukt = create_bigquery_task()

    # Set task dependency
    build_sql >> process_poolbasisprodukt