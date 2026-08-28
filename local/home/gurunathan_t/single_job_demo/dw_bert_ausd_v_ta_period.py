"""
DAG: dw_bert_ausd_v_ta_period
Description: Mirror Carmen period definitions. This DAG represents the migration of the UC4 job
             DW.BERT_AUSD_V_TA_PERIOD. Since it was an orphaned UNIX job without a parent
             workflow in the source extraction, it is configured as a standalone, on-demand DAG.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# GLOBAL (environment-wide) variables
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None)
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=None)
HOME = os.environ.get("HOME", "/home/airflow")

# JOB-SPECIFIC variables
DWH_JOB_KENNUNG = 'AUSD_V_TA_PERIOD'

DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# REVIEW: This extraction is an orphaned JOBS_UNIX task with no parent JOBP workflow or
# scheduling definition. It has been wrapped in a standalone Airflow DAG with schedule=None.
# Identify how this script is triggered in the wider UC4 environment and integrate it into
# the parent DAG or schedule accordingly.
with DAG(
    dag_id='dw_bert_ausd_v_ta_period',
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated_uc4', 'standalone_jobs_unix'],
) as dag:

    # Task representing the execution of the KornShell wrapper script
    # Original target command: &HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh
    # Original variable context: DWH_JOB_KENNUNG='AUSD_V_TA_PERIOD'
    dw_bert_ausd_v_ta_period_task = BashOperator(
        task_id='dw_bert_ausd_v_ta_period',
        bash_command=f"python {HOME}/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.py",
        env={
            'DWH_JOB_KENNUNG': DWH_JOB_KENNUNG,
            'HOME': HOME,
            'GCP_PROJECT': GCP_PROJECT,
            'BQ_DATASET': Variable.get('BQ_DATASET', default_var=''),
            'CARMEN_STAGE_DATASET': Variable.get('CARMEN_STAGE_DATASET', default_var=''),
            'BERT_DIR_ROOT': os.environ.get('BERT_DIR_ROOT', f'{HOME}/SQL/aktuell'),
            'DW_DIR_UTL': os.environ.get('DW_DIR_UTL', '/tmp'),
        },
    )

    dw_bert_ausd_v_ta_period_task