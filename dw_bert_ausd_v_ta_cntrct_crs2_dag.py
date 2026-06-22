# Legacy Sources:
#   - UC4 Job Definition: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_CNTRCT_CRS2.xml
#   - Wrapper KornShell Script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh
#   - Core KornShell Script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
# Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS2
#
# This Airflow DAG orchestrates the migration of the DW.BERT_AUSD_V_TA_CNTRCT_CRS2
# job from its legacy UC4/KornShell/Oracle environment to Google BigQuery.
# It handles the truncation of the target table, fetches a dynamic date parameter,
# and executes the core data transformation logic in BigQuery Standard SQL.

import pendulum
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Define project and dataset IDs as placeholders.
# In a real-world Airflow deployment, these would typically be managed via:
# - Airflow Variables (e.g., Variable.get("source_project_id"))
# - Environment variables
# - Airflow Connections (e.g., defining project in the 'google_cloud_default' conn)
# For this example, they are hardcoded for clarity, but should be replaced
# with actual values or more dynamic retrieval methods.
SOURCE_PROJECT_ID = "your-gcp-source-project-id"
SOURCE_DATASET = "source_dataset"
DWH_PROJECT_ID = "your-gcp-dwh-project-id"
DWH_DATASET = "dwh_dataset"

# --- Python functions for PythonOperator tasks ---

def truncate_target_table_func(project_id: str, dataset_id: str, table_id: str):
    """
    Python callable to truncate the target BigQuery table.
    This replaces the Oracle procedure call `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`
    for table truncation.
    """
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    truncate_sql = f"TRUNCATE TABLE `{project_id}.{dataset_id}.{table_id}`"
    print(f"Executing BigQuery DDL: {truncate_sql}")
    bq_hook.run_query(sql=truncate_sql, use_legacy_sql=False)
    print(f"BigQuery table {project_id}.{dataset_id}.{table_id} truncated successfully.")

def get_v_datum_func(project_id: str, dataset_id: str, **context):
    """
    Python callable to fetch the 'v_datum' parameter from BigQuery.
    This replaces the Oracle SQL query against `isbert_schema.dwtk_meldungen`.
    The fetched value is pushed to Airflow XCom for potential downstream use.
    """
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    query_sql = f"""
        SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') AS s_datum
        FROM `{project_id}.{dataset_id}.dwtk_meldungen` AS m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    print(f"Executing BigQuery query to get v_datum: {query_sql}")
    df = bq_hook.get_pandas_df(sql=query_sql, dialect='standard')

    # Extract the s_datum value
    v_datum = df['s_datum'].iloc[0] if not df.empty and 's_datum' in df.columns else '19000101'
    print(f"Fetched v_datum: {v_datum}")

    # Push v_datum to XCom, making it accessible to other tasks (e.g., via `ti.xcom_pull`)
    task_instance = context['ti']
    task_instance.xcom_push(key='v_datum', value=v_datum)

    return v_datum

# --- DAG Definition ---

with DAG(
    dag_id='dw_bert_ausd_v_ta_cntrct_crs2',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval=None, # Define your schedule here, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=['dwh', 'bert', 'contract', 'migration'],
    params={
        "source_project_id": SOURCE_PROJECT_ID,
        "source_dataset": SOURCE_DATASET,
        "dwh_project_id": DWH_PROJECT_ID,
        "dwh_dataset": DWH_DATASET,
    },
    doc_md="""
    ### Airflow DAG for DW.BERT_AUSD_V_TA_CNTRCT_CRS2 Migration

    This DAG replaces the legacy UC4 job, KornShell scripts, and Oracle SQL
    for `DW.BERT_AUSD_V_TA_CNTRCT_CRS2`. Its purpose is to update contract data
    by reconciling and populating the `ta_cntrct_crs2` table in BigQuery.

    **Flow:**
    1. Truncate the target `ta_cntrct_crs2` table.
    2. Fetch a dynamic `v_datum` parameter from `dwtk_meldungen`.
    3. Execute the core data transformation (INSERT INTO SELECT)
       from `ta_cntrct_crs` into `ta_cntrct_crs2`.

    **Legacy Components Replaced:**
    - UC4 Job Definition (`DW.BERT_AUSD_V_TA_CNTRCT_CRS2.xml`)
    - Wrapper KornShell Script (`r_ausd_v_ta_cntrct_crs2.ksh`)
    - Core KornShell Script (`k_ausd_v_ta_cntrct_crs2.ksh`)
    - Oracle SQL Script (`d_ausd_v_ta_cntrct_crs2.sql`)
    """
) as dag:
    start_job = DummyOperator(
        task_id='start_job',
    )

    truncate_target_table = PythonOperator(
        task_id='truncate_target_table',
        python_callable=truncate_target_table_func,
        op_kwargs={
            'project_id': DWH_PROJECT_ID,
            'dataset_id': DWH_DATASET,
            'table_id': 'ta_cntrct_crs2'
        },
        gcp_conn_id='google_cloud_default',
    )

    get_v_datum = PythonOperator(
        task_id='get_v_datum',
        python_callable=get_v_datum_func,
        op_kwargs={
            'project_id': SOURCE_PROJECT_ID,
            'dataset_id': SOURCE_DATASET
        },
        provide_context=True, # Required to access 'ti' (TaskInstance) for XCom
        gcp_conn_id='google_cloud_default',
    )

    execute_main_sql = BigQueryExecuteQueryOperator(
        task_id='execute_main_sql',
        sql='d_ausd_v_ta_cntrct_crs2.bqsql', # Refers to the SQL file created
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
        # 'params' from the DAG are automatically passed to the SQL file for Jinja templating.
        # Since the SQL file contains an 'INSERT INTO' DML statement,
        # 'destination_dataset_table' and 'write_disposition' are not used here.
    )

    end_job = DummyOperator(
        task_id='end_job',
    )

    # --- Define Task Dependencies ---
    start_job >> truncate_target_table
    truncate_target_table >> get_v_datum
    get_v_datum >> execute_main_sql
    execute_main_sql >> end_job