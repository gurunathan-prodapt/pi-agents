# This Airflow DAG replaces the legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh

from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# SQL logic encapsulated in a single Python function
def build_discount_rr_sql():
    # Create target table if needed and load data in a single BigQuery statement
    # NOTE: Consider 'WRITE_TRUNCATE' for 'create_disposition' if 'TRUNCATE TABLE' is explicitly needed before insert.
    sql = """
            CREATE TABLE IF NOT EXISTS `your_project.your_dataset.sof_ta_p_discount_rr` (
              cntrct_id STRING,
              discount_id STRING,
              disc_vector_ty STRING,
              cntrct_obj_version STRING,
              cntrct_template_id STRING,
              disc_invoice_item_id STRING,
              rabatt NUMERIC,
              rabatthoehe NUMERIC,
              rabattierte_rech_pos STRING,
              contract_number STRING,
              std_vertrag STRING
            );

            INSERT INTO `your_project.your_dataset.sof_ta_p_discount_rr` (
              cntrct_id,
              discount_id,
              disc_vector_ty,
              cntrct_obj_version,
              cntrct_template_id,
              disc_invoice_item_id,
              rabatt,
              rabatthoehe,
              rabattierte_rech_pos,
              contract_number,
              std_vertrag
            )
            SELECT
              da.cntrct_id,
              da.discount_id,
              da.disc_vector_ty,
              da.cntrct_obj_version,
              da.cntrct_template_id,
              da.disc_invoice_item_id,
              da.rabatt,
              da.rabatthoehe,
              da.rabattierte_rech_pos,
              c.contract_number,
              ct.cds_description AS std_vertrag
            FROM `your_project.your_dataset.sof_ta_discount_rr` AS da
            JOIN `your_project.your_dataset.sof_ta_cntrct_crs` AS c
              ON da.cntrct_id = c.cntrct_id
             AND da.cntrct_obj_version = c.obj_version
            JOIN `your_project.your_dataset.sof_ta_cntrct_templ` AS ct
              ON da.cntrct_template_id = ct.cntrct_template_id;
            """
    # Incorporate logic for 'v_datum' (Stichtag) and 'tmpFile' (v_records) if required.
    # Example:
    # sql += """
    # INSERT INTO `your_project.your_dataset.process_metadata` (job_id, records_processed)
    # SELECT 'k_ausd_v_ta_p_discount_rr', COUNT(*) FROM `your_project.your_dataset.sof_ta_p_discount_rr`;
    # """
    return sql

# DAG definition
with DAG(
    dag_id="k_ausd_v_ta_p_discount_rr",
    default_args=default_args,
    description="BigQuery processing for discount RR data",
    schedule_interval=None, # Define schedule if applicable, e.g., '0 0 * * *' for daily
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "discount", "etl"],
) as dag:

    # Start marker
    start = DummyOperator(
        task_id="start"
    )

    # Single BigQuery task executing the full SQL logic
    process_discount_rr = BigQueryExecuteQueryOperator(
        task_id="process_discount_rr",
        sql=build_discount_rr_sql(),
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED", # Can be set to 'CREATE_NEVER' if table is pre-created
        write_disposition="WRITE_APPEND", # Consider 'WRITE_TRUNCATE' if the previous run's data needs to be cleared
        location="US", # Adjust to your BigQuery region
    )

    # End marker
    end = DummyOperator(
        task_id="end"
    )

    # Task dependencies
    start >> process_discount_rr >> end