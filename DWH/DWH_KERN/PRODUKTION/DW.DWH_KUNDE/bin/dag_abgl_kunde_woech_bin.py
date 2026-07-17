"""
Apache Airflow Bin Action Script: dag_abgl_kunde_woech_bin
Mirrors the folder integrity rule from:
DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/dag_abgl_kunde_woech_bin.py
"""

import logging
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def evaluate_run_discrepancies(logical_date, **kwargs):
    """
    Queries BigQuery to check for anomalies and route the branch step.
    """
    project_id = Variable.get("GCP_PROJECT")
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    stichtag_str = logical_date.strftime("%Y%m%d")
    
    sql = f"""
        SELECT COUNT(1) as total_mismatches 
        FROM `{project_id}.work.wrk_kunden_abweichungen`
        WHERE stichtag = PARSE_DATE('%Y%m%d', '{stichtag_str}')
    """
    
    records = bq_hook.get_first(sql)
    l_Abweichungen = records[0] if records else 0
    
    ti = kwargs['ti']
    ti.xcom_push(key='l_Abweichungen', value=l_Abweichungen)
    
    if l_Abweichungen > 0:
        return 'warning_notification_task'
    return 'completion_notification_task'


def log_warning_message(logical_date, **kwargs):
    """
    Preserves and outputs the literal warning message EXACTLY as required.
    """
    ti = kwargs['ti']
    l_Abweichungen = ti.xcom_pull(task_ids='evaluate_metrics', key='l_Abweichungen')
    l_Stichtag = logical_date.strftime("%Y%m%d")
    gcs_log_bucket = Variable.get("GCS_LOG_BUCKET")
    Protokoll_Datei = f"gs://{gcs_log_bucket}/logs/abgl_kunde_{l_Stichtag}.log"
    
    # RULE: Preserve exact literal warning message including dynamic parameters
    warning_str = f"[W] {l_Abweichungen} Abweichungen im Kundenadressabgleich gefunden, siehe {Protokoll_Datei}"
    
    logging.warning(warning_str)
    print(warning_str)


def log_completion_message(logical_date, **kwargs):
    """
    Preserves and outputs the literal completion message EXACTLY as required.
    """
    LAUF_WOCHE = logical_date.strftime("%Y-%W")
    
    # RULE: Preserve exact literal completion message of execution graphs
    completion_str = f"Kundenadressabgleich fuer Lauf {LAUF_WOCHE} angestossen"
    
    logging.info(completion_str)
    print(completion_str)