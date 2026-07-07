from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False, 
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dw_bert_ausd_bp_ta_msisdn_his',
    default_args=default_args,
    description='Executes MSISDN history generation and sync',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # In a modern BigQuery-native workflow, we execute the transformation script as a multi-statement query block.
    # This matches the legacy execution pattern previously orchestrated via KornShell wrapper scripts.
    execute_msisdn_history_update = BigQueryInsertJobOperator(
        task_id='execute_msisdn_history_update',
        configuration={
            "query": {
                "query": """
                    DECLARE v_job_name STRING DEFAULT 'BERT_DROP_TEMP_TABLE';
                    DECLARE v_default_date STRING DEFAULT '19000101';
                    DECLARE v_datum STRING;
                    DECLARE v_process_date DATE;

                    SET v_datum = (
                      SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), v_default_date)
                      FROM `{{ var.value.gcp_project_id }}.isbert_schema.dwtk_meldungen` m
                      WHERE m.job_kennung = v_job_name
                    );

                    SET v_process_date = PARSE_DATE('%Y%m%d', v_datum);

                    TRUNCATE TABLE `{{ var.value.gcp_project_id }}.sof.ta_msisdn_his`;

                    INSERT INTO `{{ var.value.gcp_project_id }}.sof.ta_msisdn_his`
                    (
                      bpri_com_id,
                      msisdn,
                      callnumber_role_id,
                      valid_to
                    )
                    SELECT
                      cn1.bpri_com_id,
                      CONCAT(CAST(cn1.cc AS STRING), CAST(cn1.ndc AS STRING), CAST(cn1.sn AS STRING)) AS msisdn,
                      cn1.callnumber_role_id,
                      cn1.valid_to
                    FROM `{{ var.value.gcp_project_id }}.pds.ta_callnumber` cn1
                    WHERE DATE(cn1.insert_at) <= v_process_date
                      AND (
                        cn1.modified_at IS NULL
                        OR DATE(cn1.modified_at) > v_process_date
                      )
                      AND DATE(cn1.valid_from) <= v_process_date
                      AND cn1.is_production = 1;
                """,
                "useLegacySql": False,
            }
        }
    )