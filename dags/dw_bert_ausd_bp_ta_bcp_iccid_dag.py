# Apache Airflow DAG for DW.BERT_AUSD_BP_TA_BCP_ICCID
# Replaces:
# - vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BCP_ICCID.xml
# - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh
# - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh
# - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_iccid.sql

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime

# Define GCP Project and Dataset IDs using Airflow Variables for flexibility
# Ensure 'gcp_project_id' and 'bigquery_dataset_id' are set in Airflow Variables
GCP_PROJECT_ID = "{{ var.value.gcp_project_id }}"
BIGQUERY_DATASET_ID = "{{ var.value.bigquery_dataset_id }}"

def _extract_v_datum_func(ti, project_id, dataset_id):
    """
    Extracts the v_datum (max timecreated) from dwtk_meldungen and pushes it to XCom.
    """
    hook = BigQueryHook(gcp_conn_id='google_cloud_default', project_id=project_id)
    client = hook.get_client()
    query = f"""
        SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(timecreated)), '19000101')
        FROM `{project_id}.{dataset_id}.dwtk_meldungen`
        WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    query_job = client.query(query)
    results = query_job.result()
    v_datum = None
    for row in results:
        v_datum = row[0]
        break
    if v_datum is None:
        # Fallback if no matching rows are found, though IFNULL should handle initial MAX(NULL)
        v_datum = '19000101'
    ti.xcom_push(key='v_datum', value=v_datum)
    print(f"Extracted v_datum: {v_datum}")
    return v_datum

with DAG(
    dag_id='dw_bert_ausd_bp_ta_bcp_iccid',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, # Set to appropriate schedule or '@daily' for daily runs
    catchup=False,
    tags=['bert', 'bigquery', 'data_ingestion'],
    params={
        'stichtag': '{{ ds_nodash }}',  # Default to current date in YYYYMMDD format
        'wiederanlaufwert': ''  # Default empty, can be overridden during manual trigger
    }
) as dag:
    # Task 1: Retrieve v_datum from dwtk_meldungen
    extract_v_datum_task = PythonOperator(
        task_id='extract_v_datum',
        python_callable=_extract_v_datum_func,
        op_kwargs={
            'project_id': GCP_PROJECT_ID,
            'dataset_id': BIGQUERY_DATASET_ID
        }
    )

    # Task 2: Truncate and Load (or INSERT OVERWRITE) into sof_ta_bcp_iccid
    # Note: The design document flags the core SQL for 'retire' migration bucket,
    # suggesting business value re-evaluation. This implementation directly translates
    # the existing logic to BigQuery.
    # The 'stichtag' and 'wiederanlaufwert' parameters are defined for the DAG,
    # but the provided core SQL does not directly use them for filtering the main load.
    # If business logic requires them, modifications to the SQL below would be needed.
    transform_load_data_task = BigQueryExecuteQueryOperator(
        task_id='transform_load_data',
        sql=f"""
            INSERT OVERWRITE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_bcp_iccid`
            (CNTRCT_ID,
             BPR_ID,
             CNTRCT_ID_REF,
             TN_ICCID,
             TN_IMSI_HLR)
            SELECT DISTINCT
                bp.cntrct_id,
                bp.bpr_id,
                bp.cntrct_id_ref,
                ic.tn_iccid,
                ic.tn_imsi_hlr
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_bpr_bcp` AS bp
            JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sof_ta_iccid_vertrag` AS ic
              ON bp.cntrct_id_ref = ic.cntrct_id;
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
    )

    extract_v_datum_task >> transform_load_data_task
---