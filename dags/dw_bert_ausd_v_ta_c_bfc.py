# Legacy source: DW.BERT_AUSD_V_TA_C_BFC.xml, r_ausd_v_ta_c_bfc.ksh, k_ausd_v_ta_c_bfc.ksh
# Job: DW.BERT_AUSD_V_TA_C_BFC

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

with DAG(
    dag_id='dw_bert_ausd_v_ta_c_bfc',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # As per design: "The Airflow DAG will initially be unscheduled"
    catchup=False,
    tags=['bigquery', 'etl'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0, # As per design: "error handling and explicit restart notes ... require manual review to determine the appropriate Airflow retry strategy and error handling callbacks. The current Airflow design proposes retries=0."
    },
    template_searchpath=["/opt/airflow/dags/sql"], # Assumes SQL files are in a 'sql' subdirectory within the DAGs folder
) as dag:
    execute_bigquery_script = BigQueryInsertJobOperator(
        task_id='execute_d_ausd_v_ta_c_bfc_sql',
        configuration={
            "query": {
                "query": "{% include 'd_ausd_v_ta_c_bfc.bqsql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        gcp_conn_id='google_cloud_default', # Ensure this connection exists in Airflow
        # Pass project and dataset information as parameters to the Jinja-templated SQL file
        params={
            'project': 'your-gcp-project-id', # Placeholder for GCP Project ID
            'dataset': 'your_bigquery_dataset' # Placeholder for BigQuery Dataset
        },
        # For templated fields, refer to: https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/bigquery/index.html#airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator
        # The 'query' field is templated by default.
    )

    # Future tasks could be added here, e.g., for logging, notifications, or dependent jobs.
    # The design document implies a single main task for this migration.