# Airflow DAG for k_ausd_v_ta_discount.ksh
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh
# This DAG orchestrates the execution of the BigQuery Stored Procedure.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# Define your project and dataset IDs
PROJECT_ID = 'your-gcp-project-id'  # Replace with your GCP project ID
DATASET_ID = 'your_dataset_id'      # Replace with your BigQuery dataset ID

with DAG(
    dag_id='k_ausd_v_ta_discount_bq_dag',
    start_date=days_ago(1),
    schedule_interval='@daily',  # Adjust schedule as needed
    catchup=False,
    tags=['bigquery', 'etl'],
    description='Airflow DAG to trigger BigQuery Stored Procedure for k_ausd_v_ta_discount.ksh migration',
) as dag:
    # Example values for parameters.
    # In a real scenario, these might come from Airflow Variables, XComs, or dynamic generation.
    job_kennung = 'BERT_DISCOUNT_JOB_001'
    eintrags_nr = 'ENTRY_001'

    call_bq_stored_procedure = BigQueryInsertJobOperator(
        task_id='call_bq_stored_procedure',
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');",
                "useLegacySql": False,
                "queryParameters": [
                    {"name": "p_JobKennung", "parameterType": {"type": "STRING"}, "parameterValue": {"value": job_kennung}},
                    {"name": "p_EintragsNr", "parameterType": {"type": "STRING"}, "parameterValue": {"value": eintrags_nr}},
                ]
            }
        },
        gcp_conn_id='google_cloud_default',  # Ensure you have a BigQuery connection defined in Airflow
    )