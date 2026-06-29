"""
Airflow DAG Orchestration for ausd_bp_ta_ibcp_ccid.
Legacy Source: Unix Shell (KSH) & Oracle PL/SQL.
Target: BigQuery ELT and Data Reconciliation.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.transfers.jdbc_to_gcs import JdbcToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Environment Configuration Constants
GCS_BUCKET = "prj-ausd-stage-gcp-raw-data"
ORACLE_CONN_ID = "oracle_erp_jdbc_conn"
GCP_CONN_ID = "google_cloud_default"

STAGING_DATASET = "bq_stage_ta"
CORE_DATASET = "finance_ta"
PROJECT_ID_STAGE = "prj-ausd-stage-gcp"
PROJECT_ID_CORE = "prj-ausd-core-gcp"

default_args = {
    "owner": "AUSD Finance Analytics Team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["ausd_finance_ops@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def run_reconciliation_audit(**context):
    """
    Executes the reconciliation audits, checking if staging matches core records.
    Throws AirflowException to mark DAG run failed if discrepancies are present.
    """
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    sql = """
    WITH staging_metrics AS (
      SELECT 
        'dim_ccid_ibcp' AS table_name,
        COUNT(*) AS staging_row_count,
        COUNT(DISTINCT code_combination_id) AS staging_distinct_keys,
        0 AS staging_total_amount
      FROM `prj-ausd-stage-gcp.bq_stage_ta.stg_oracle_ccid`
      WHERE segment1 IN ('AU', '080')
      UNION ALL
      SELECT
        'fact_ibcp_ledger' AS table_name,
        COUNT(*) AS staging_row_count,
        COUNT(DISTINCT txn_id) AS staging_distinct_keys,
        SUM(COALESCE(txn_amount, 0)) AS staging_total_amount
      FROM `prj-ausd-stage-gcp.bq_stage_ta.stg_ibcp_txns`
      WHERE entity_code IN ('AU', '080')
    ),
    core_metrics AS (
      SELECT 
        'dim_ccid_ibcp' AS table_name,
        COUNT(*) AS core_row_count,
        COUNT(DISTINCT code_combination_id) AS core_distinct_keys,
        0 AS core_total_amount
      FROM `prj-ausd-core-gcp.finance_ta.dim_ccid_ibcp`
      WHERE entity_code IN ('AU', '080')
      UNION ALL
      SELECT
        'fact_ibcp_ledger' AS table_name,
        COUNT(*) AS core_row_count,
        COUNT(DISTINCT txn_id) AS core_distinct_keys,
        SUM(COALESCE(txn_amount, 0)) AS core_total_amount
      FROM `prj-ausd-core-gcp.finance_ta.fact_ibcp_ledger`
      WHERE entity_code IN ('AU', '080')
    )
    SELECT 
      s.table_name,
      s.staging_row_count,
      c.core_row_count,
      (s.staging_row_count - c.core_row_count) AS row_count_delta,
      s.staging_distinct_keys,
      c.core_distinct_keys,
      (s.staging_distinct_keys - c.core_distinct_keys) AS distinct_keys_delta,
      s.staging_total_amount,
      c.core_total_amount,
      (s.staging_total_amount - c.core_total_amount) AS total_amount_delta
    FROM staging_metrics s
    JOIN core_metrics c ON s.table_name = c.table_name;
    """
    records = bq_hook.get_records(sql)
    for record in records:
        table_name = record[0]
        row_delta = record[3]
        key_delta = record[6]
        amount_delta = record[9]
        
        if row_delta != 0 or key_delta != 0 or amount_delta != 0:
            raise AirflowException(
                f"Reconciliation error detected on table {table_name}: "
                f"Row Count Delta = {row_delta}, "
                f"Key Distinct Delta = {key_delta}, "
                f"Amount Delta = {amount_delta}. Execution halting!"
            )
    print("Reconciliation step completed successfully for all CCID and transaction targets.")


with DAG(
    dag_id="ausd_bp_ta_ibcp_ccid",
    default_args=default_args,
    description="ELT Pipeline for AU GL Combinations and Intercompany Transaction ledgering",
    schedule_interval="0 4 * * *",  # Runs daily at 04:00 AM UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. JDBC extraction tasks pulling from Oracle database straight to GCS
    extract_ccids_to_gcs = JdbcToGCSOperator(
        task_id="extract_ccids_to_gcs",
        sql="""
            SELECT 
              ccid AS code_combination_id,
              segment1,
              segment2,
              segment3,
              segment4,
              segment5,
              summary_flag,
              enabled_flag,
              TO_CHAR(start_date_active, 'YYYY-MM-DD') AS start_date_active,
              TO_CHAR(end_date_active, 'YYYY-MM-DD') AS end_date_active
            FROM GL_CODE_COMBINATIONS
            WHERE segment1 IN ('AU', '080')
        """,
        bucket=GCS_BUCKET,
        object_name="oracle_gl/ccids_{{ ds_nodash }}.parquet",
        jdbc_conn_id=ORACLE_CONN_ID,
        gcp_conn_id=GCP_CONN_ID,
        export_format="parquet",
    )

    extract_txns_to_gcs = JdbcToGCSOperator(
        task_id="extract_txns_to_gcs",
        sql="""
            SELECT 
              txn_id,
              ccid AS code_combination_id,
              entity_code,
              cost_center_code,
              account_code,
              sub_account_code,
              intercompany_partner_code,
              txn_amount,
              txn_currency,
              TO_CHAR(txn_date, 'YYYY-MM-DD') AS txn_date
            FROM IBCP_STAGE_TXN
            WHERE entity_code IN ('AU', '080')
        """,
        bucket=GCS_BUCKET,
        object_name="oracle_ibcp/txns_{{ ds_nodash }}.parquet",
        jdbc_conn_id=ORACLE_CONN_ID,
        gcp_conn_id=GCP_CONN_ID,
        export_format="parquet",
    )

    # 2. BigQuery Staging Loading Tasks
    load_ccids_to_staging = GCSToBigQueryOperator(
        task_id="load_ccids_to_staging",
        bucket=GCS_BUCKET,
        source_objects=["oracle_gl/ccids_{{ ds_nodash }}.parquet"],
        destination_project_dataset_table=f"{PROJECT_ID_STAGE}.{STAGING_DATASET}.stg_oracle_ccid",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",  # Fully refresh current day's active staging snapshot
        gcp_conn_id=GCP_CONN_ID,
    )

    load_txns_to_staging = GCSToBigQueryOperator(
        task_id="load_txns_to_staging",
        bucket=GCS_BUCKET,
        source_objects=["oracle_ibcp/txns_{{ ds_nodash }}.parquet"],
        destination_project_dataset_table=f"{PROJECT_ID_STAGE}.{STAGING_DATASET}.stg_ibcp_txns",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND",  # Append daily financial ledger transactions
        gcp_conn_id=GCP_CONN_ID,
    )

    # 3. BigQuery Transform and Merge execution tasks
    run_merge_transformations = BigQueryInsertJobOperator(
        task_id="run_merge_transformations",
        configuration={
            "query": {
                "query": "{% include '../sql/merge_ccid_ibcp.sql' %}",
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # 4. Final step execution of Reconciliation checks to prevent data leaks or omissions
    reconcile_and_verify = PythonOperator(
        task_id="reconcile_and_verify",
        python_callable=run_reconciliation_audit,
        provide_context=True,
    )

    # Sequence Mapping: Parallel extract & load, then SQL transformation, followed by strict Reconciliation
    [extract_ccids_to_gcs >> load_ccids_to_staging, extract_txns_to_gcs >> load_txns_to_staging] >> run_merge_transformations >> reconcile_and_verify