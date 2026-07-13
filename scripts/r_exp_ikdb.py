from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

METADATA_DATASET = "metadata_dataset"
TRACKING_TABLE = f"{METADATA_DATASET}.dwtk_meldungen"

def execute_ikdb_export(
    task_id: str, 
    job_name: str, 
    sql_file_path: str, 
    export_target_gcs: str,
    gcp_conn_id: str = 'google_cloud_default'
) -> TaskGroup:
    temp_bq_table = f"temporary_staging_dataset.{job_name.lower()}_temp"

    with TaskGroup(group_id=task_id) as export_group:

        check_already_run = BigQueryValueCheckOperator(
            task_id='check_prior_run_registration',
            sql=f"""
                SELECT COUNT(1) 
                FROM `{TRACKING_TABLE}`
                WHERE job_name = '{job_name}' 
                  AND execution_date = '{{{{ ds }}}}' 
                  AND status = 'SUCCESS'
            """,
            pass_value=0,
            gcp_conn_id=gcp_conn_id
        )

        execute_transformation_query = BigQueryInsertJobOperator(
            task_id='run_export_query_to_temp_table',
            configuration={
                "query": {
                    "query": f"""
                        -- Stubbed implementation of missing d_ikdb_exp_stamm.sql
                        CREATE OR REPLACE TABLE `{temp_bq_table}` AS
                        SELECT 
                          master_id,
                          name,
                          record_date,
                          CURRENT_TIMESTAMP() as processed_at
                        FROM `analytical_dataset.source_stamm`
                        WHERE record_date = '{{{{ ds }}}}';
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=gcp_conn_id
        )

        export_temp_table_to_gcs = BigQueryToGCSOperator(
            task_id='export_table_to_gcs',
            source_project_dataset_table=temp_bq_table,
            destination_cloud_storage_uris=[export_target_gcs],
            export_format='CSV',
            field_delimiter=';',
            gcp_conn_id=gcp_conn_id
        )

        log_successful_run = BigQueryInsertJobOperator(
            task_id='log_run_metadata_success',
            configuration={
                "query": {
                    "query": f"""
                        INSERT INTO `{TRACKING_TABLE}` (job_name, execution_date, status, updated_timestamp)
                        VALUES ('{job_name}', '{{{{ ds }}}}', 'SUCCESS', CURRENT_TIMESTAMP());
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=gcp_conn_id
        )

        check_already_run >> execute_transformation_query >> export_temp_table_to_gcs >> log_successful_run

    return export_group