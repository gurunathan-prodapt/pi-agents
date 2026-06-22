# Legacy Source: JOBP:DW.DWH_APT_EXPORT_TAEGLICH_JP (child of DW.BERT_ABLAUFSTEUERUNG)
# Job: DW.BERT_ABLAUFSTEUERUNG
# Airflow TaskGroup for daily APT exports.

from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
import os

def create_dwh_apt_export_taeglich_jp_taskgroup(dag):
    with TaskGroup("dwh_apt_export_taeglich_jp", dag=dag) as dwh_apt_export_taeglich_jp_group:
        start_apt_export = DummyOperator(task_id='start_apt_export')

        # Task to execute bert_bestandsdaten.sql
        bert_bestandsdaten_task = BigQueryExecuteQueryOperator(
            task_id='bert_bestandsdaten',
            sql='sql/bert_bestandsdaten.sql',
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default',
            # The destination table is defined within the SQL script using CREATE OR REPLACE TABLE
        )

        # Task to execute bert_nna_daten.sql
        bert_nna_daten_task = BigQueryExecuteQueryOperator(
            task_id='bert_nna_daten',
            sql='sql/bert_nna_daten.sql',
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default',
        )

        # Task to execute bert_nna_voice.sql
        bert_nna_voice_task = BigQueryExecuteQueryOperator(
            task_id='bert_nna_voice',
            sql='sql/bert_nna_voice.sql',
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default',
        )

        # Task to execute bert_rabattdaten.sql
        bert_rabattdaten_task = BigQueryExecuteQueryOperator(
            task_id='bert_rabattdaten',
            sql='sql/bert_rabattdaten.sql',
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default',
        )

        # Task for r_exis_v2 re-implementation (exporting data from BQ to GCS)
        # This calls the scripts/r_exis_v2.py script using a PythonOperator and os.system
        # This allows Airflow templating (e.g., ds_nodash) to be passed to the external script.
        export_bestandsdaten_to_gcs = PythonOperator(
            task_id='export_bestandsdaten_to_gcs',
            python_callable=lambda **kwargs: os.system(f"""
                python {os.path.abspath(os.path.dirname(__file__) + "/../../scripts/r_exis_v2.py")} \
                --project_id {{ var.value.TARGET_BQ_PROJECT }} \
                --dataset_id {{ var.value.TARGET_BQ_DATASET }} \
                --table_id bert_bestandsdaten \
                --gcs_bucket_name {{ var.value.GCS_EXPORT_BUCKET }} \
                --gcs_destination_path exports/bert_bestandsdaten_{{ ds_nodash }}.csv
            """),
        )

        export_nna_daten_to_gcs = PythonOperator(
            task_id='export_nna_daten_to_gcs',
            python_callable=lambda **kwargs: os.system(f"""
                python {os.path.abspath(os.path.dirname(__file__) + "/../../scripts/r_exis_v2.py")} \
                --project_id {{ var.value.TARGET_BQ_PROJECT }} \
                --dataset_id {{ var.value.TARGET_BQ_DATASET }} \
                --table_id bert_nna_daten \
                --gcs_bucket_name {{ var.value.GCS_EXPORT_BUCKET }} \
                --gcs_destination_path exports/bert_nna_daten_{{ ds_nodash }}.csv
            """),
        )

        export_nna_voice_to_gcs = PythonOperator(
            task_id='export_nna_voice_to_gcs',
            python_callable=lambda **kwargs: os.system(f"""
                python {os.path.abspath(os.path.dirname(__file__) + "/../../scripts/r_exis_v2.py")} \
                --project_id {{ var.value.TARGET_BQ_PROJECT }} \
                --dataset_id {{ var.value.TARGET_BQ_DATASET }} \
                --table_id bert_nna_voice \
                --gcs_bucket_name {{ var.value.GCS_EXPORT_BUCKET }} \
                --gcs_destination_path exports/bert_nna_voice_{{ ds_nodash }}.csv
            """),
        )

        export_rabattdaten_to_gcs = PythonOperator(
            task_id='export_rabattdaten_to_gcs',
            python_callable=lambda **kwargs: os.system(f"""
                python {os.path.abspath(os.path.dirname(__file__) + "/../../scripts/r_exis_v2.py")} \
                --project_id {{ var.value.TARGET_BQ_PROJECT }} \
                --dataset_id {{ var.value.TARGET_BQ_DATASET }} \
                --table_id bert_rabattdaten \
                --gcs_bucket_name {{ var.value.GCS_EXPORT_BUCKET }} \
                --gcs_destination_path exports/bert_rabattdaten_{{ ds_nodash }}.csv
            """),
        )

        start_apt_export >> [
            bert_bestandsdaten_task,
            bert_nna_daten_task,
            bert_nna_voice_task,
            bert_rabattdaten_task
        ]

        bert_bestandsdaten_task >> export_bestandsdaten_to_gcs
        bert_nna_daten_task >> export_nna_daten_to_gcs
        bert_nna_voice_task >> export_nna_voice_to_gcs
        bert_rabattdaten_task >> export_rabattdaten_to_gcs

    return dwh_apt_export_taeglich_jp_group