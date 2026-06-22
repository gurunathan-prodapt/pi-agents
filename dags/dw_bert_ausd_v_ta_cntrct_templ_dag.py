# DW.BERT_AUSD_V_TA_CNTRCT_TEMPL - Airflow DAG
# Legacy Sources:
# - vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.xml
# - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
# - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh
# - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_templ.sql

"""
Airflow DAG for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.

This DAG migrates the legacy UC4/Automic + KornShell + Oracle SQL ETL for mirroring
Carmen contract templates into Apache Airflow on GCP. It runs on the provided cron
schedule, loads configuration, determines the processing date (v_datum) from the
migrated metadata table, extracts the two source tables into BigQuery staging, runs
the BigQuery transformation/load logic for the final contract template table, and
then updates job run and watermark control tables.
"""

from __future__ import annotations

import pendulum
import os
import sys

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Add the parent directory of this DAG file to the sys.path
# This allows importing `python.utils` and `python.data_ingestion`
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(DAG_FOLDER, "..")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

import python.utils as utils
import python.data_ingestion as data_ingestion

# Load the config file for DAG parameters
CONFIG_FILE_PATH = os.path.join(PROJECT_ROOT, "config", "config.yaml")
CONFIG = utils.load_yaml_config(CONFIG_FILE_PATH)

DAG_ID = "dw_bert_ausd_v_ta_cntrct_templ_dag"
SCHEDULE_CRON = CONFIG.get("airflow_schedule_interval", "0 1 * * *")
DEFAULT_ARGS = CONFIG.get("airflow_default_args", {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay_seconds": 300,
})

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Mirror Carmen contract templates into BigQuery curated table.",
    schedule=SCHEDULE_CRON,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["migration", "dw", "bert", "contract_templates"],
) as dag:
    start = DummyOperator(task_id="start")

    # The config is already loaded and accessible as CONFIG directly
    # However, if any task needed to pull dynamic config, it could be pushed via XCom.
    # For now, `load_configuration` is merged with DAG instantiation.

    get_processing_date_task = PythonOperator(
        task_id="get_processing_date",
        python_callable=utils.get_processing_date_from_bq,
        op_kwargs={
            "gcp_project_id": CONFIG["gcp_project_id"],
            "job_kennung": CONFIG["metadata_dwtk_meldungen_job_kennung"],
        },
        do_xcom_push=True,
    )

    extract_cds_ta_cntrct_template_task = PythonOperator(
        task_id="extract_cds_ta_cntrct_template",
        python_callable=data_ingestion.extract_cds_ta_cntrct_template_to_bq,
        op_kwargs={
            "gcp_project_id": CONFIG["gcp_project_id"],
            "oracle_conn_id": CONFIG["oracle_conn_id"],
        },
    )

    extract_cds_ta_care_description_task = PythonOperator(
        task_id="extract_cds_ta_care_description",
        python_callable=data_ingestion.extract_cds_ta_care_description_to_bq,
        op_kwargs={
            "gcp_project_id": CONFIG["gcp_project_id"],
            "oracle_conn_id": CONFIG["oracle_conn_id"],
        },
    )

    transform_and_load_data_task = BigQueryInsertJobOperator(
        task_id="transform_and_load_data",
        project_id=CONFIG["gcp_project_id"],
        configuration={
            "query": {
                "query": "{% include 'sql/d_ausd_v_ta_cntrct_templ_transformed.sql' %}",
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "gcp_project_id",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": CONFIG["gcp_project_id"]},
                    },
                    {
                        "name": "v_datum",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ task_instance.xcom_pull(task_ids='get_processing_date') }}"},
                    },
                ],
                "destinationTable": {
                    "projectId": CONFIG["gcp_project_id"],
                    "datasetId": CONFIG["bigquery_dataset_curated"],
                    "tableId": CONFIG["target_final_fact_table"],
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
            }
        },
    )

    update_job_run_status_task = BigQueryInsertJobOperator(
        task_id="update_job_run_status",
        project_id=CONFIG["gcp_project_id"],
        configuration={
            "query": {
                "query": f"""
                INSERT INTO `{CONFIG["gcp_project_id"]}.control.etl_job_run`
                (job_id, run_id, start_time, end_time, status, message)
                VALUES (
                  'DW.BERT_AUSD_V_TA_CNTRCT_TEMPL',
                  '{{{{ dag_run.run_id }}}}',
                  '{{{{ dag_run.start_date }}}}',
                  CURRENT_TIMESTAMP(),
                  'SUCCESS',
                  'Job completed successfully'
                );
                """,
                "useLegacySql": False,
            }
        },
    )

    update_watermark_task = BigQueryInsertJobOperator(
        task_id="update_watermark",
        project_id=CONFIG["gcp_project_id"],
        configuration={
            "query": {
                "query": f"""
                MERGE INTO `{CONFIG["gcp_project_id"]}.control.etl_watermark` AS T
                USING (
                    SELECT
                        '{CONFIG["target_final_fact_table"]}' AS table_name,
                        '{{{{ task_instance.xcom_pull(task_ids='get_processing_date') }}}}' AS new_watermark_value,
                        CURRENT_TIMESTAMP() AS updated_at
                ) AS S
                ON T.table_name = S.table_name
                WHEN MATCHED THEN
                    UPDATE SET last_watermark_value = PARSE_DATE('%Y%m%d', S.new_watermark_value), updated_at = S.updated_at
                WHEN NOT MATCHED THEN
                    INSERT (table_name, watermark_column, last_watermark_value, updated_at)
                    VALUES (S.table_name, 'v_datum', PARSE_DATE('%Y%m%d', S.new_watermark_value), S.updated_at);
                """,
                "useLegacySql": False,
            }
        },
    )

    end = DummyOperator(task_id="end")

    # Task dependencies
    start >> get_processing_date_task
    get_processing_date_task >> [extract_cds_ta_cntrct_template_task, extract_cds_ta_care_description_task]
    [extract_cds_ta_cntrct_template_task, extract_cds_ta_care_description_task] >> transform_and_load_data_task
    transform_and_load_data_task >> [update_job_run_status_task, update_watermark_task]
    [update_job_run_status_task, update_watermark_task] >> end