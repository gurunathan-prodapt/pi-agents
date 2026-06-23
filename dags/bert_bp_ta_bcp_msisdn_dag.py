# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Define project_id as a placeholder. Replace with your actual GCP Project ID.
PROJECT_ID = "gcp-project-id"

@dag(
    dag_id="bert_bp_ta_bcp_msisdn_dag",
    schedule=None,  # Adjust schedule as needed (e.g., "@daily", "0 0 * * *")
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bert", "bigquery", "etl"],
    params={
        "stichtag": {
            "type": "string",
            "title": "Stichtag (Cutoff Date)",
            "description": "Date in YYYYMMDD format. Defaults to yesterday if not provided.",
            "default": "",
        },
        "wiederanlaufwert": {
            "type": "string",
            "title": "Wiederanlaufwert (Restart Value)",
            "description": "A value used for restart logic. Defaults to empty string.",
            "default": "",
        },
    }
)
def bert_bp_ta_bcp_msisdn():
    """
    This DAG migrates the 'Bereitstellung Basisprodukte BERT' job to BigQuery.
    It prepares contract cache data for demand scoring by selecting and
    inserting data into the target table `sof_schema.ta_bcp_msisdn_bq`.
    """

    @task
    def get_stichtag_and_wiederanlaufwert(**kwargs):
        """
        Parses DAG parameters for stichtag and wiederanlaufwert.
        If stichtag is not provided, it defaults to yesterday's date.
        """
        stichtag_param = kwargs["params"].get("stichtag")
        wiederanlaufwert_param = kwargs["params"].get("wiederanlaufwert")

        if not stichtag_param:
            # Default to yesterday's date in YYYYMMDD format
            stichtag_param = (pendulum.today("UTC") - pendulum.duration(days=1)).strftime("%Y%m%d")

        kwargs["ti"].xcom_push(key="stichtag", value=stichtag_param)
        kwargs["ti"].xcom_push(key="wiederanlaufwert", value=wiederanlaufwert_param)
        print(f"Using stichtag: {stichtag_param}")
        print(f"Using wiederanlaufwert: {wiederanlaufwert_param}")

    @task
    def retrieve_s_datum(**kwargs):
        """
        Retrieves the s_datum value from isbert_schema.dwtk_meldungen_bq
        for job control.
        """
        bigquery_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        client = bigquery_hook.get_client(project_id=PROJECT_ID)

        query = f"""
            SELECT
                COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') AS s_datum
            FROM
                `{PROJECT_ID}.isbert_schema.dwtk_meldungen_bq` m
            WHERE
                m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        """
        query_job = client.query(query)
        rows = query_job.result()
        s_datum = None
        for row in rows:
            s_datum = row["s_datum"]
        
        if s_datum is None:
            # Fallback if no record is found, as per original logic's '19000101' default
            s_datum = '19000101'

        kwargs["ti"].xcom_push(key="s_datum", value=s_datum)
        print(f"Retrieved s_datum: {s_datum}")

    parse_params = get_stichtag_and_wiederanlaufwert()
    get_s_datum = retrieve_s_datum()

    # Task to truncate the target table
    truncate_target_table = BigQueryExecuteQueryOperator(
        task_id="truncate_ta_bcp_msisdn_bq",
        sql=f"TRUNCATE TABLE `{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq`;",
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
    )

    # Task to execute the main data transformation (INSERT INTO SELECT)
    insert_transformed_data = BigQueryInsertJobOperator(
        task_id="insert_data_into_target",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT_ID}.sof_schema.ta_bcp_msisdn_bq`
                    (CNTRCT_ID,
                     BPR_ID,
                     CNTRCT_ID_REF,
                     TN_TEL_MSISDN)
                    SELECT
                        DISTINCT
                        bp.CNTRCT_ID,
                        bp.BPR_ID,
                        bp.CNTRCT_ID_REF,
                        rn.TN_TEL_MSISDN
                    FROM
                        `{PROJECT_ID}.sof_schema.ta_bpr_bcp_bq` AS bp
                    INNER JOIN
                        `{PROJECT_ID}.sof_schema.ta_rn_vertrag_bq` AS rn
                    ON
                        bp.CNTRCT_ID_REF = rn.CNTRCT_ID;
                """,
                "useLegacySql": False,
                "writeDisposition": "WRITE_APPEND", # INSERT INTO assumes append
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": "sof_schema",
                    "tableId": "ta_bcp_msisdn_bq",
                },
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # Define task dependencies
    parse_params >> get_s_datum >> truncate_target_table >> insert_transformed_data

bert_bp_ta_bcp_msisdn()
---