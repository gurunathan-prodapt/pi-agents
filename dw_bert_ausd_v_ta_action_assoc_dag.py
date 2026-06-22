# Airflow DAG for k_ausd_v_ta_action_assoc.ksh migration
# Replaces legacy KornShell script and d_ausd_v_ta_action_assoc.sql
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Set to a specific, non-dynamic date for production
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # For robust error handling, implement a custom on_failure_callback function
    # 'on_failure_callback': some_failure_handler_function
}

with DAG(
    dag_id='dw_bert_ausd_v_ta_action_assoc_dag',
    default_args=default_args,
    description='Orchestrates the BigQuery update for ta_action_assoc data.',
    schedule_interval=None,  # Original job had no explicit schedule. Define based on business needs.
    catchup=False,
    tags=['bigquery', 'etl', 'migration', 'ta_action_assoc'],
) as dag:
    # BigQuery SQL to update the sof_ta_action_assoc table
    # This SQL block incorporates the date determination, truncation,
    # and data insertion logic from the original d_ausd_v_ta_action_assoc.sql.
    # Table names are fully qualified as per the migration design document.
    execute_ta_action_assoc_update = BigQueryOperator(
        task_id='execute_ta_action_assoc_update',
        sql="""
BEGIN
  DECLARE v_datum STRING;
  SET v_datum = (
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM `my_gcp_project.my_dataset.dwtk_meldungen` m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  -- Truncate existing data in the target table
  TRUNCATE TABLE `my_gcp_project.my_dataset.sof_ta_action_assoc`;

  -- Insert new data based on filtered source records
  INSERT INTO `my_gcp_project.my_dataset.sof_ta_action_assoc`(cntrct_id, rv_action_id)
  SELECT
    ac.cntrct_id,
    ac.rv_action_id
  FROM `my_gcp_project.my_dataset.cds_ta_action_assoc` ac
  WHERE DATE(ac.insert_at) <= PARSE_DATE('%Y%m%d', v_datum)
    AND DATE(ac.valid_from) <= PARSE_DATE('%Y%m%d', v_datum)
    AND ac.is_production = 1
    AND (ac.modified_at IS NULL OR DATE(ac.modified_at) > PARSE_DATE('%Y%m%d', v_datum))
    AND (ac.valid_to IS NULL OR DATE(ac.valid_to) > PARSE_DATE('%Y%m%d', v_datum));
END;
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',  # Ensure this Google Cloud connection is configured in Airflow
    )

    # Define task dependencies
    # In this simple case, there's only one task.
    # Additional tasks for pre-checks, data quality, or notifications can be added here.
```