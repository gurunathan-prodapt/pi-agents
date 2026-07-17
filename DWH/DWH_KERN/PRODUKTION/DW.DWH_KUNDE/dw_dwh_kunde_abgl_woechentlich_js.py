import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

default_args = {
    'owner': 'dw_dwh_kunde',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def execute_reconciliation_script(**kwargs) -> None:
    """
    Airflow task wrapper that mimics command-line script invocation.
    """
    lauf_woche = kwargs.get('ds_nodash', 'UNDEFINED')
    print(f"Kundenadressabgleich fuer Lauf {lauf_woche} angestossen")
    
    os.environ["SQL_FILE_PATH"] = "/home/airflow/gcs/dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql"
    sys.path.append("/home/airflow/gcs/dags")
    
    from DWH.DWH_KERN.PRODUKTION.DW.DWH_KUNDE.bin import r_abgl_kunde_woech
    
    sys_argv_backup = sys.argv
    sys.argv = [r_abgl_kunde_woech.__file__, "-s", lauf_woche]
    
    try:
        r_abgl_kunde_woech.main()
    finally:
        sys.argv = sys_argv_backup


with DAG(
    'dw_dwh_kunde_abgl_woechentlich_js',
    default_args=default_args,
    description='Weekly customer address reconciliation workflow',
    schedule_interval='0 4 * * 1',  # Weekly on Monday at 04:00 AM
    catchup=False,
) as dag:

    run_reconciliation = PythonOperator(
        task_id='run_reconciliation_process',
        python_callable=execute_reconciliation_script,
        provide_context=True,
    )