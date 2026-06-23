# Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh
#
# Apache Airflow DAG for synchronizing contract data into BigQuery.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "project_id": "your-gcp-project-id", # TODO: Replace with your actual GCP Project ID
    "bigquery_conn_id": "google_cloud_default", # Ensure this connection is configured in Airflow
}

with DAG(
    dag_id="dag_ta_p_vertrag_sync",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule interval, e.g., "@daily" or "0 0 * * *"
    catchup=False,
    tags=["bigquery", "etl", "contract_data"],
    params={
        "JobKennung": "BERT_TA_P_VERTRAG", # Default value, can be overridden at runtime
        "EintragsNr": "12345", # Default value, can be overridden at runtime
    },
    default_args=default_args,
) as dag:

    def _log_v_datum(**kwargs):
        """Logs the derived v_datum value."""
        ti = kwargs["ti"]
        v_datum_value = ti.xcom_pull(task_ids="get_v_datum_task")
        print(f"Derived v_datum: {v_datum_value}")

    # Task to determine v_datum
    # In Oracle, this was SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') FROM isbert_schema.dwtk_meldungen
    get_v_datum_task = BigQueryExecuteQueryOperator(
        task_id="get_v_datum_task",
        sql="""
            SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
            FROM `isbert_dwh.dwtk_meldungen` m
            WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        """,
        use_legacy_sql=False,
        gcp_conn_id="bigquery_conn_id",
        do_xcom_push=True, # Push the result (single value) to XCom
    )

    log_v_datum_task = PythonOperator(
        task_id="log_v_datum_task",
        python_callable=_log_v_datum,
    )

    # Task to truncate the target table
    truncate_target_table_task = BigQueryExecuteQueryOperator(
        task_id="truncate_target_table_task",
        sql="TRUNCATE TABLE `sof_dwh.ta_p_vertrag`;",
        use_legacy_sql=False,
        gcp_conn_id="bigquery_conn_id",
    )

    # Task for the main data transformation (INSERT INTO SELECT)
    # The SQL is loaded from the `bq_d_ausd_v_ta_p_vertrag.sql` file
    main_insert_task = BigQueryInsertJobOperator(
        task_id="main_insert_task",
        configuration={
            "query": {
                "query": "{% include 'bq_d_ausd_v_ta_p_vertrag.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": default_args["project_id"],
                    "datasetId": "sof_dwh",
                    "tableId": "ta_p_vertrag",
                },
                "writeDisposition": "WRITE_APPEND", # INSERT INTO should append, truncate handles overwrite
            }
        },
        gcp_conn_id="bigquery_conn_id",
    )

    # Task group for truncating various temporary tables
    with TaskGroup("truncate_temp_tables_group") as truncate_temp_tables_group:
        temp_tables_to_truncate = [
            "sof_dwh.ta_disc_zusgf",
            "sof_dwh.ta_discount",
            "sof_dwh.ta_barrier_zusgf",
            "sof_dwh.ta_barrier",
            "sof_dwh.ta_cntrct_crs",
            "sof_dwh.ta_cntrct_templ",
            "sof_dwh.ta_cntrct_valid",
            "sof_dwh.ta_period",
            "sof_dwh.ta_bp_ref",
            "sof_dwh.ta_inv_assign",
            "sof_dwh.ta_inv_def",
            "sof_dwh.ta_acc_ref",
            "sof_dwh.ta_notice",
            "sof_dwh.ta_apn_ve",
            "sof_dwh.ta_discount_rr",
            "sof_dwh.ta_vvl_dwh",
            "sof_dwh.ta_vvl_upgrade",
            "sof_dwh.ta_cntrct_crs2",
            "sof_dwh.ta_cntrct_crs3",
            "sof_dwh.ta_inv_acc",
            "sof_dwh.ta_vertrag_tmp", # Truncated at the end, as in original script
            "sof_dwh.ta_action_assoc",
        ]

        for i, table_name in enumerate(temp_tables_to_truncate):
            BigQueryExecuteQueryOperator(
                task_id=f"truncate_{table_name.replace('.', '_')}",
                sql=f"TRUNCATE TABLE `{table_name}`;",
                use_legacy_sql=False,
                gcp_conn_id="bigquery_conn_id",
            )

    # Define task dependencies
    get_v_datum_task >> log_v_datum_task >> truncate_target_table_task >> main_insert_task >> truncate_temp_tables_group