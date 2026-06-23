# Apache Airflow DAG to orchestrate the BigQuery Stored Procedures
# Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
# This DAG demonstrates the orchestration. You might need to adjust
# schedule, default_args, and parameter passing as per your environment.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Define BigQuery project and dataset for consistency
BIGQUERY_PROJECT = 'your_project_id'
BIGQUERY_DATASET = 'your_dataset_id'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

with DAG(
    dag_id='k_ausd_bp_ta_bpr_apn_migration_dag',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None, # Define your schedule here, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=['bigquery', 'etl', 'migration'],
    default_args=default_args,
    doc_md="""
    ### Airflow DAG for k_ausd_bp_ta_bpr_apn Migration

    This DAG orchestrates the BigQuery Stored Procedure that replaces the
    legacy KornShell script `k_ausd_bp_ta_bpr_apn.ksh`.
    It handles parameter passing, execution, and optional post-processing.
    """,
) as dag:
    start_task = EmptyOperator(task_id='start')

    # Define parameters for the main BigQuery Stored Procedure
    # These can be passed via Airflow variables, XComs, or hardcoded for testing.
    # For a real-world scenario, consider using Airflow's params or dynamic values.
    job_kennung_param = 'DEFAULT_JOB' # Corresponds to -j
    eintrags_nr_param = 'DEFAULT_ENTRY_001' # Corresponds to -f
    stichtag_param = pendulum.today().format('DDMMYYYY') # Corresponds to -s
    wiederanlauf_wert_param = 0 # Corresponds to -l

    # Task to call the main orchestration BigQuery Stored Procedure
    call_main_sp = BigQueryInsertJobOperator(
        task_id='call_main_orchestration_sp',
        configuration={
            "query": {
                "query": f"""
                    CALL `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.sp_k_ausd_bp_ta_bpr_apn`(
                        p_JobKennung => @job_kennung,
                        p_EintragsNr => @eintrags_nr,
                        p_Stichtag => @stichtag,
                        p_wiederanlaufWert => @wiederanlauf_wert
                    );
                """,
                "useLegacySql": False,
                "queryParameters": [
                    {"name": "job_kennung", "parameterType": {"type": "STRING"}, "parameterValue": {"value": job_kennung_param}},
                    {"name": "eintrags_nr", "parameterType": {"type": "STRING"}, "parameterValue": {"value": eintrags_nr_param}},
                    {"name": "stichtag", "parameterType": {"type": "STRING"}, "parameterValue": {"value": stichtag_param}},
                    {"name": "wiederanlauf_wert", "parameterType": {"type": "INT64"}, "parameterValue": {"value": str(wiederanlauf_wert_param)}},
                ],
            }
        },
        gcp_conn_id='google_cloud_default', # Ensure this connection is configured in Airflow
    )

    # Optional task for post-processing if the commented logic is activated.
    # This task will run the SQL script to create the cibasisprodukt table.
    call_post_processing_sql = BigQueryInsertJobOperator(
        task_id='run_cibasisprodukt_post_processing',
        configuration={
            "query": {
                "query": f"""
                    -- This SQL script creates the cibasisprodukt table
                    -- based on the logic from the commented-out section in the original ksh script.
                    -- Ensure `your_project_id.your_dataset_id.cibasis_data24`,
                    -- `your_project_id.your_dataset_id.cibasis_data96`, and
                    -- `your_project_id.your_dataset_id.cibasis_fax` tables exist and are populated.

                    -- Intermediate result 1: Cleaned and distinct cibasis_data24
                    CREATE OR REPLACE TEMPORARY TABLE `cibasis_data24_cleaned` AS
                    SELECT DISTINCT
                        REPLACE(id, ' ', '') AS id,
                        REPLACE(data_field_24_1, ' ', '') AS data_field_24_1,
                        REPLACE(data_field_24_2, ' ', '') AS data_field_24_2
                    FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.cibasis_data24`;

                    -- Intermediate result 2: Cleaned and distinct cibasis_data96
                    CREATE OR REPLACE TEMPORARY TABLE `cibasis_data96_cleaned` AS
                    SELECT DISTINCT
                        REPLACE(id, ' ', '') AS id,
                        REPLACE(data_field_96_1, ' ', '') AS data_field_96_1,
                        REPLACE(data_field_96_2, ' ', '') AS data_field_96_2
                    FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.cibasis_data96`;

                    -- Intermediate result 3: Cleaned and distinct cibasis_fax
                    CREATE OR REPLACE TEMPORARY TABLE `cibasis_fax_cleaned` AS
                    SELECT DISTINCT
                        REPLACE(id, ' ', '') AS id,
                        REPLACE(fax_data, ' ', '') AS fax_data
                    FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.cibasis_fax`;


                    -- First join: cibasis_data24 and cibasis_data96
                    CREATE OR REPLACE TEMPORARY TABLE `cibasis_24_96_tmp` AS
                    SELECT
                        COALESCE(d24.id, d96.id) AS id, -- Key from either table
                        d24.data_field_24_1,
                        d24.data_field_24_2,
                        d96.data_field_96_1,
                        d96.data_field_96_2
                    FROM
                        `cibasis_data24_cleaned` d24
                    FULL OUTER JOIN
                        `cibasis_data96_cleaned` d96
                    ON
                        d24.id = d96.id;

                    -- Second join: cibasis_24_96_tmp with cibasis_fax
                    CREATE OR REPLACE TABLE `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.cibasisprodukt` AS
                    SELECT
                        tmp.id,
                        tmp.data_field_24_1,
                        tmp.data_field_24_2,
                        tmp.data_field_96_1,
                        tmp.data_field_96_2,
                        fax.fax_data
                    FROM
                        `cibasis_24_96_tmp` tmp
                    LEFT OUTER JOIN
                        `cibasis_fax_cleaned` fax
                    ON
                        tmp.id = fax.id;
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
    )


    end_task = EmptyOperator(task_id='end')

    start_task >> call_main_sp >> call_post_processing_sql >> end_task
    # If post-processing is not needed, uncomment the line below and comment the one above:
    # start_task >> call_main_sp >> end_task