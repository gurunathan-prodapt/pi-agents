# Apache Airflow DAG for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
# Replaces legacy KornShell scripts r_ausd_v_ta_cntrct_templ.ksh and k_ausd_v_ta_cntrct_templ.ksh
# Orchestrates BigQuery SQL transformation from d_ausd_v_ta_cntrct_templ.sql

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# TODO: Replace with your actual BigQuery project and dataset IDs
BIGQUERY_PROJECT_ID = "project"  # e.g., "your-gcp-project-id"
BIGQUERY_DATASET_ID = "dataset"  # e.g., "your_data_warehouse_dataset"

# Define target and source tables using the placeholders
TARGET_TABLE = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_cntrct_templ"
DWTK_MELDUNGEN_TABLE = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.dwtk_meldungen"
CDS_TA_CNTRCT_TEMPLATE_TABLE = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.cds_ta_cntrct_template"
CDS_TA_CARE_DESCRIPTION_TABLE = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.cds_ta_care_description"

with DAG(
    dag_id="r_ausd_v_ta_cntrct_templ_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule, e.g., "@daily", "0 0 * * *"
    catchup=False,
    tags=["bigquery", "etl"],
    description="Migrates r_ausd_v_ta_cntrct_templ.ksh workflow to BigQuery and Airflow",
) as dag:
    # Task 1: Extract v_datum (processing date)
    # This task determines the processing date from dwtk_meldungen and pushes it to XCom.
    extract_v_datum_task = BigQueryExecuteQueryOperator(
        task_id="extract_v_datum",
        sql=f"""
            SELECT COALESCE(FORMAT_DATE('%Y-%m-%d', MAX(DATE(m.timecreated))), '1900-01-01')
            FROM `{DWTK_MELDUNGEN_TABLE}` m
            WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        """,
        use_legacy_sql=False,
        do_xcom_push=True,
        # The result of this query (single row, single column) will be pushed to XCom.
        # It can be retrieved as `ti.xcom_pull(task_ids='extract_v_datum')`.
        # Note: If no rows match, COALESCE will return '1900-01-01'.
    )

    # Task 2: Truncate the target table
    # This task truncates the `sof_ta_cntrct_templ` table before inserting new data.
    truncate_target_table_task = BigQueryExecuteQueryOperator(
        task_id="truncate_target_table",
        sql=f"TRUNCATE TABLE `{TARGET_TABLE}`;",
        use_legacy_sql=False,
    )

    # Task 3: Insert transformed data into the target table
    # This task executes the main transformation logic, using v_datum from XCom.
    insert_transformed_data_task = BigQueryExecuteQueryOperator(
        task_id="insert_transformed_data",
        sql=f"""
            INSERT INTO `{TARGET_TABLE}`
            (
              cntrct_template_id,
              cds_description_id,
              cds_description
            )
            SELECT
              ct.cntrct_template_id,
              ct.cds_description_id,
              cd.cds_description
            FROM `{CDS_TA_CNTRCT_TEMPLATE_TABLE}` ct
            JOIN `{CDS_TA_CARE_DESCRIPTION_TABLE}` cd
              ON ct.cds_description_id = cd.cds_description_id
            WHERE ct.insert_at <= DATE('{{{{ task_instance.xcom_pull(task_ids='extract_v_datum') }}}}')
              AND (ct.modified_at IS NULL OR ct.modified_at > DATE('{{{{ task_instance.xcom_pull(task_ids='extract_v_datum') }}}}'))
              AND ct.valid_from <= DATE('{{{{ task_instance.xcom_pull(task_ids='extract_v_datum') }}}}')
              AND (ct.valid_to IS NULL OR ct.valid_to > DATE('{{{{ task_instance.xcom_pull(task_ids='extract_v_datum') }}}}'))
              AND ct.is_production = 1
              AND cd.language = 1;
        """,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",  # Ensure this connection ID is configured in Airflow
    )

    # Define task dependencies
    extract_v_datum_task >> truncate_target_table_task >> insert_transformed_data_task