from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# The SQL statements to execute on BigQuery
sql_query = """
TRUNCATE TABLE `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_cntrct_dist`;

INSERT INTO `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_cntrct_dist` (cntrct_id)
SELECT DISTINCT
  cntrct_id
FROM `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_bpr_basis`
WHERE cntrct_id IS NOT NULL;
"""

with DAG(
    dag_id="ausd_bp_ta_cntrct_dist",
    default_args=default_args,
    description="Load distinct contract IDs into sof_ta_cntrct_dist",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "etl", "contract", "distinct"],
) as dag:

    start = EmptyOperator(task_id="start")

    load_contract_distinct = BigQueryInsertJobOperator(
        task_id="load_contract_distinct",
        configuration={
            "query": {
                "query": sql_query,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="{{ var.value.gcp_conn_id }}",
        location="{{ var.value.bq_location }}",
    )

    end = EmptyOperator(task_id="end")

    start >> load_contract_distinct >> end