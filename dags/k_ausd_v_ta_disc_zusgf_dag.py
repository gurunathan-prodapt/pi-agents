# Airflow DAG for k_ausd_v_ta_disc_zusgf.ksh
# Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh
# Orchestrates BigQuery transformations defined in bigquery/sql/d_ausd_v_ta_disc_zusgf_transformation.sql

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='k_ausd_v_ta_disc_zusgf_dag',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # This DAG is typically triggered manually or by an upstream process
    catchup=False,
    tags=['bigquery', 'etl', 'discount'],
    params={
        "job_kennung": {
            "type": "string",
            "title": "Job Identifier",
            "description": "Identifier for the job execution (e.g., from original -j parameter)",
            "default": "DEFAULT_JOB",
        },
        "entry_nr": {
            "type": "string",
            "title": "Entry Number",
            "description": "Entry number (e.g., from original -f parameter)",
            "default": "0",
        },
    },
    doc_md="""
    ### k_ausd_v_ta_disc_zusgf_dag
    This DAG migrates the functionality of the legacy KornShell script `k_ausd_v_ta_disc_zusgf.ksh`.
    It orchestrates BigQuery operations to prepare and consolidate discount data into `raw_sof.sof$ta_disc_zusgf`.

    **Purpose**: Prepare and consolidate discount data.
    **Source**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh`
    **Target Platform**: BigQuery & Cloud Composer.
    """,
) as dag:
    start_task = DummyOperator(
        task_id='start',
    )

    # Task to truncate the target table
    # This replaces DWPA_UTIL_SKRIPT.runstatement('TRUNCATE TABLE sof$ta_disc_zusgf')
    # NOTE: Replace `your_gcp_project_id` with your actual Google Cloud Project ID.
    truncate_target_table = BigQueryExecuteQueryOperator(
        task_id='truncate_target_table',
        sql="TRUNCATE TABLE `your_gcp_project_id.raw_sof.sof$ta_disc_zusgf`;",
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default', # Ensure this connection is configured in Airflow
    )

    # Task to execute the main transformation logic
    # This replaces the SQL*Plus execution of d_ausd_v_ta_disc_zusgf.sql
    # NOTE: Replace `your_gcp_project_id` with your actual Google Cloud Project ID for all table references.
    execute_main_transformation = BigQueryExecuteQueryOperator(
        task_id='execute_main_transformation',
        sql="""
            DECLARE v_datum STRING DEFAULT (
              SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
              FROM `your_gcp_project_id.isbert_schema.dwtk_meldungen` m
              WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
            );

            INSERT INTO `your_gcp_project_id.raw_sof.sof$ta_disc_zusgf`
              (cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt_alle)
            WITH dzg AS (
              SELECT DISTINCT
                CAST(cntrct_id AS INT64) AS cntrct_id,
                CAST(cntrct_obj_version AS INT64) AS cntrct_obj_version,
                disc_vector_ty
              FROM `your_gcp_project_id.raw_sof.sof$ta_discount`
            ),
            con AS (
              SELECT
                CAST(cntrct_id AS INT64) AS cntrct_id,
                CAST(cntrct_obj_version AS INT64) AS cntrct_obj_version,
                STRING_AGG(rabatt_text, ', ' ORDER BY rabatt_text) AS rabatt_alle
              FROM (
                SELECT DISTINCT
                  CAST(cntrct_id AS INT64) AS cntrct_id,
                  CAST(cntrct_obj_version AS INT64) AS cntrct_obj_version,
                  CONCAT(CAST(rabatt AS STRING), ' (', CAST(rabatthoehe AS STRING), '%)') AS rabatt_text
                FROM `your_gcp_project_id.raw_sof.sof$ta_discount`
              )
              GROUP BY cntrct_id, cntrct_obj_version
            )
            SELECT
              dzg.cntrct_id,
              dzg.cntrct_obj_version,
              dzg.disc_vector_ty,
              con.rabatt_alle
            FROM dzg
            LEFT JOIN con
              ON dzg.cntrct_id = con.cntrct_id
             AND dzg.cntrct_obj_version = con.cntrct_obj_version;
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default', # Ensure this connection is configured in Airflow
    )

    # Define task dependencies
    start_task >> truncate_target_table >> execute_main_transformation