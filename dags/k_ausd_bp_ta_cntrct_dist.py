"""
Airflow DAG: k_ausd_bp_ta_cntrct_dist
Orchestrates parameter validation, dynamic date derivation, and triggers the
BigQuery stored procedure sp_k_ausd_bp_ta_cntrct_dist, replacing k_ausd_bp_ta_cntrct_dist.ksh.
"""

from datetime import datetime, timedelta
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Configuration mapped from System Migration Context
PROJECT_ID = "gcp-project-id"
DATASET = "isbert_schema"
AUDIT_DATASET = "audit_log"
CONN_ID = "google_cloud_default"

def validate_and_prepare_parameters(**kwargs):
    """
    Validates required parameters and computes runtime dates (today, yesterday).
    Expected dag_run.conf fields:
      - job_kennung (defaults to PoolBasisprodukt if missing)
      - eintrags_nr
      - stichtag (format: DDMMYYYY)
      - wiederanlauf_wert (defaults to 0 if missing)
    """
    conf = kwargs.get('dag_run').conf if (kwargs.get('dag_run') and kwargs.get('dag_run').conf) else {}
    
    job_kennung = conf.get('job_kennung', 'PoolBasisprodukt')
    eintrags_nr = conf.get('eintrags_nr')
    stichtag = conf.get('stichtag')
    wiederanlauf_wert = conf.get('wiederanlauf_wert', 0)
    
    errors = []
    if not job_kennung:
        errors.append("Jobkennung fehlt (job_kennung)")
    if not eintrags_nr:
        errors.append("EintragsNr fehlt (eintrags_nr)")
    if not stichtag:
        errors.append("Stichtag fehlt (stichtag)")
        
    if errors:
        error_msg = "; ".join(errors)
        _log_error_to_bq(job_kennung or "PoolBasisprodukt", eintrags_nr or "unknown", stichtag or "unknown", error_msg)
        raise AirflowFailException(f"Validation failed: {error_msg}")
        
    # Date Validation
    if not re.match(r"^\d{8}$", stichtag):
        error_msg = f"Ungueltiges Datum: {stichtag}. Format DDMMYYYY erforderlich."
        _log_error_to_bq(job_kennung, eintrags_nr, stichtag, error_msg)
        raise AirflowFailException(error_msg)
        
    try:
        # Validate calendar date validity
        datetime.strptime(stichtag, "%d%m%Y")
    except ValueError:
        error_msg = f"Ungueltiges Datum: {stichtag} ist kein gueltiger Kalendertag."
        _log_error_to_bq(job_kennung, eintrags_nr, stichtag, error_msg)
        raise AirflowFailException(error_msg)
        
    # Calculate yesterday and today dynamically
    today_dt = datetime.now()
    yesterday_dt = today_dt - timedelta(days=1)
    
    p_datum_heute = today_dt.strftime("%d%m%Y")
    p_datum_gestern = yesterday_dt.strftime("%d%m%Y")
    
    # Push parameters to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='job_kennung', value=job_kennung)
    ti.xcom_push(key='eintrags_nr', value=eintrags_nr)
    ti.xcom_push(key='stichtag', value=stichtag)
    ti.xcom_push(key='wiederanlauf_wert', value=int(wiederanlauf_wert))
    ti.xcom_push(key='p_datum_heute', value=p_datum_heute)
    ti.xcom_push(key='p_datum_gestern', value=p_datum_gestern)

def _log_error_to_bq(job_name, entry_nr, stichtag, error_message):
    """
    Direct logging to audit log dataset in BigQuery for validation failures
    """
    try:
        hook = BigQueryHook(gcp_conn_id=CONN_ID)
        client = hook.get_client()
        table_ref = f"{PROJECT_ID}.{AUDIT_DATASET}.job_error_log"
        rows_to_insert = [{
            "job_name": job_name,
            "entry_nr": entry_nr,
            "stichtag": stichtag,
            "error_message": error_message,
            "created_at": datetime.utcnow().isoformat()
        }]
        client.insert_rows_json(table_ref, rows_to_insert)
    except Exception as e:
        print(f"Warning: Could not write error log to BigQuery: {str(e)}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'k_ausd_bp_ta_cntrct_dist',
    default_args=default_args,
    description='Orchestrates k_ausd_bp_ta_cntrct_dist BQ stored procedure execution.',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['isbert', 'migration'],
) as dag:

    validate_params = PythonOperator(
        task_id='validate_params',
        python_callable=validate_and_prepare_parameters,
        provide_context=True,
    )

    execute_procedure = BigQueryInsertJobOperator(
        task_id='execute_stored_procedure',
        gcp_conn_id=CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CALL `{PROJECT_ID}.{DATASET}.sp_k_ausd_bp_ta_cntrct_dist`(
                        '{{{{ ti.xcom_pull(task_ids="validate_params", key="job_kennung") }}}}',
                        '{{{{ ti.xcom_pull(task_ids="validate_params", key="eintrags_nr") }}}}',
                        '{{{{ ti.xcom_pull(task_ids="validate_params", key="stichtag") }}}}',
                        {{{{ ti.xcom_pull(task_ids="validate_params", key="wiederanlauf_wert") }}}},
                        '{{{{ ti.xcom_pull(task_ids="validate_params", key="p_datum_heute") }}}}',
                        '{{{{ ti.xcom_pull(task_ids="validate_params", key="p_datum_gestern") }}}}'
                    );
                """,
                "useLegacySql": False,
            }
        },
        location="EU"
    )

    validate_params >> execute_procedure