"""
Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh
This DAG demonstrates how to calculate reporting date parameters dynamically
using Airflow macro variables (ds_nodash, yesterday_ds_nodash, logical_date).
These match the logic previously provided by yesterday.ksh (gestern.ksh).
"""

from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Configure local reporting timezone as Central European Time
local_tz = pendulum.timezone("Europe/Berlin")

default_args = {
    'owner': 'isbert',
    'start_date': datetime(2023, 1, 1, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='isbert_reporting_dates_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['isbert', 'legacy_gestern_ksh'],
) as dag:

    # Example downstream query execution demonstrating macro injection for dates
    run_reporting_job = BigQueryInsertJobOperator(
        task_id='run_reporting_job',
        configuration={
            "query": {
                # Injects date values computed via Airflow macros matching gestern.ksh outputs
                "query": """
                    SELECT 
                        * 
                    FROM 
                        `isbert_aufbereitung.v_reporting_dates`
                    WHERE 
                        -- {{ ds_nodash }} represents Var_Datum_Heute (YYYYMMDD)
                        -- {{ yesterday_ds_nodash }} represents Var_Datum_Gestern (YYYYMMDD)
                        -- {{ logical_date.strftime('%Y%m') }} represents Var_Monat_Heute (YYYYMM)
                        -- {{ (logical_date - macros.timedelta(days=1)).strftime('%Y%m') }} represents Var_Monat_Gestern (YYYYMM)
                        Var_Datum_Heute = '{{ ds_nodash }}'
                        AND Var_Datum_Gestern = '{{ yesterday_ds_nodash }}'
                        AND Var_Monat_Heute = '{{ logical_date.strftime("%Y%m") }}'
                        AND Var_Monat_Gestern = '{{ (logical_date - macros.timedelta(days=1)).strftime("%Y%m") }}'
                """,
                "useLegacySql": False,
            }
        },
    )