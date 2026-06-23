--
-- Target code for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
--
-- This Apache Airflow DAG orchestrates the execution of the BigQuery stored procedure
-- `vertragsdatenabgleich_wrapper_sp`, replacing the original KornShell wrapper script.
--
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='vertragsdatenabgleich_vvl_upgrade_dag',
    default_args=default_args,
    description='Orchestrates BigQuery stored procedure for Vertragsdatenabgleich ta_vvl_upgrade',
    schedule_interval=None,  # Set your desired schedule interval here, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=['bigquery', 'data_reconciliation'],
) as dag:
    # Generate a unique entry number for the job run, similar to DW_EintragsNr
    # In Airflow, task instance run ID or a generated UUID could be used.
    # For simplicity, we'll use a placeholder and demonstrate how to pass it.
    # In a real scenario, you might use {{ run_id }} or a custom XCom pushed value.
    job_kennung = 'BERT_V_TA_VVL_UPGRADE'
    eintrags_nr = "{{ ts_nodash }}" # Example: using timestamp as entry number

    execute_vertragsdatenabgleich_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='execute_vertragsdatenabgleich_sp',
        project_id='your_gcp_project_id',
        dataset_id='your_bq_dataset_id',
        procedure_id='vertragsdatenabgleich_wrapper_sp',
        parameters=[
            {"name": "p_job_kennung", "parameterType": {"type": "STRING"}, "parameterValue": job_kennung},
            {"name": "p_eintragsnr", "parameterType": {"type": "STRING"}, "parameterValue": eintrags_nr},
        ],
    )