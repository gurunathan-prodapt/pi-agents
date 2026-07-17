"""
Airflow DAG orchestrating the daily invoice export (DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS).
Replaces the legacy UC4 scheduler and handles end-to-end execution.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

# Import split runner functions to maintain clean folder boundaries
from bin.r_exp_rechnung_taeglich import log_header, validate_and_log_results

# Global variables sourced via Airflow Configuration Store
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET", default_var="DWH_KERN")

DEFAULT_ARGS = {
    'owner': 'DW',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='dwh_rechnung_export_taeglich_js',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    catchup=False,
    template_searchpath=['/home/gurunathan_t/clean_migration_dataset/DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/']
) as dag:

    # Step 1: Log UC4 and Shell script initialization headers verbatim
    log_start = PythonOperator(
        task_id='log_start_verbatim',
        python_callable=log_header,
        templates_dict={'stichtag': '{{ ds_nodash }}'},
        provide_context=True,
    )

    # Step 2: Native BigQuery Extract Operation (equivalent to the SQL execution)
    execute_bq_query = BigQueryInsertJobOperator(
        task_id='execute_bq_sql_export',
        configuration={
            "query": {
                "query": "d_exp_rechnung_taeglich.sql",
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "p_Stichtag",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ ds_nodash }}"}
                    }
                ],
                "destinationTable": {
                    "projectId": GCP_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": "TEMP_RECHNUNG_EXPORT_{{ ds_nodash }}"
                },
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    )

    # Step 3: Extract BQ staging table to GCS Cloud Storage as Pipe-Delimited
    export_to_gcs = BigQueryInsertJobOperator(
        task_id='export_temp_table_to_gcs',
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": "TEMP_RECHNUNG_EXPORT_{{ ds_nodash }}"
                },
                "destinationUris": [f"gs://{GCS_BUCKET}/rechnung_export/daily/rechnung_export_{{{{ ds_nodash }}}}.csv"],
                "destinationFormat": "CSV",
                "fieldDelimiter": "|"
            }
        }
    )

    # Step 4: Validate rows and write verbatim exit logs
    validate_results = PythonOperator(
        task_id='validate_and_log_verbatim',
        python_callable=validate_and_log_results,
        templates_dict={'stichtag': '{{ ds_nodash }}', 'gcs_bucket': GCS_BUCKET},
        provide_context=True,
    )

    # Orchestration sequence
    log_start >> execute_bq_query >> export_to_gcs >> validate_results