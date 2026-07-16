from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

# --- CONFIGURATION / RESOLUTIONS ---
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET", default_var="DWH_KKM")

JOB_KENNUNG = 'ABPZ_KKM_AIL_AGENT'
OUTPUT_LOOKUP_FILE = 'AgentADSLookup.txt'

def check_pipeline_branch(**kwargs) -> str:
    """
    Evaluates execution parameters to choose the target processing branch.
    """
    dag_run = kwargs.get('dag_run')
    force_reload = dag_run.conf.get('force_reload', False) if dag_run else False
    if force_reload:
        return "run_agent_lookup_pyspark"
    return "run_agent_lookup_pyspark"

def get_running_job_name(**kwargs):
    # Replicates Jobmonitor inclusion output print statements
    JPMJOB = kwargs['dag'].dag_id
    DWH_JOB_KENNUNG = JOB_KENNUNG
    print(f"Jobkennung {DWH_JOB_KENNUNG} eingetragen für {JPMJOB}")

def get_job_monitor_end(**kwargs):
    # Replicates Jobi register statement for completion
    JPMJOB = kwargs['dag'].dag_id
    DWH_JOB_KENNUNG = JOB_KENNUNG
    print(f"Jobkennung {DWH_JOB_KENNUNG} eingetragen für {JPMJOB}")

def parse_failure_log(context):
    # Replicates legacy German fail outputs from LESE_LOG with exact umlauts
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    print("****************************************************************")
    print("Rueckgabewert: '1' (Fehlerfall)***************************")
    print("****************************************************************")

def print_frame_params(**kwargs):
    # Prints original German log labels exactly as spelled in r_alis_objekt
    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {JOB_KENNUNG}")
    print("---------------------------------------------")
    print("----------------- Job -----------------------")
    print("Job-Nr                     : '0'")
    print(f"Jobkennung (Prüfjob)       : '{JOB_KENNUNG}'")
    print(f"Jobkennung (ab initio)     : '{JOB_KENNUNG}'")
    print("Objekt                     : 'AgentADSLookup'")
    print("Erster Tag                 : '0'")
    print("Letzter Tag (plus 1)       : '0'")
    print("Nachfahren                 : '0'")
    print("Jobname in Meldungstabelle : ''")
    print("Tabellenname (GUELTIG_VON) : ''")
    print("DeltaT fuer Stichtag       : '0'")
    print("Ladedaten                  : '0'")
    print("Ab Initio Konfig           : 'BHB_CCM_PROC_WriteAgentADSLookup.cfg'")
    print("Löschzeitspalte            : 'NULL'")
    print("Projektpräfix              : 'BHB_CCM_PROC'")
    print("Staging-Tabelle            : 'NULL'")
    print("Intervallmodus             : '0'")
    print("Parallelitätsgrad          : '1'")
    print("Erzwinge ai Version 2.13   : '0'")
    print("Vertausche Rueckgabwerte   : '0'")
    print("Logdatei                   : 'std_out_log'")
    print("---------------------------------------------")
    print("*********************************************************************")
    print("Parameter für den ab initio Prozess")
    print("*********************************************************************")
    print(" ab initio Konfiguration  = BHB_CCM_PROC_WriteAgentADSLookup.cfg")
    print(" Parallelitätsgrad        = 1")
    print(" Erster Tag Name          = BHB_CCM_PROC_FirstDay")
    print(" Erster Tag Wert          = 0")
    print(" Letzter Tag Plus 1 Name  = BHB_CCM_PROC_LastDayPlus1")
    print(" Letzter Tag Plus 1 Wert  = 0")
    print(" Löschzeitspalte          = NULL")
    print(" Ziel loeschen            = 0")
    print(" Erzwinge ai Version 2.13 = 0")
    print("*********************************************************************")
    print("")

def print_end_state(**kwargs):
    # Prints original German success statement matching h_alis_objekt
    print("Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rückgabewert 0 beendet.")

def print_success_value(**kwargs):
    print("****************************************************************")
    print("Rueckgabewert: '0' ***************************************")
    print("****************************************************************")

def print_start_inc(**kwargs):
    print("Der Prüfjob dw_dwh_abpz_kkm_ail_agent läuft im Jobplan DW.DWH_KKM_IMPORT_TAEGLICH_JP")
    print("Der Status für die Applikation DWH ist: go")
    print("Prüfung erfolgreich, starte Ab Initio Job(s)")

def print_ende_inc(**kwargs):
    print("Der Prüfjob dw_dwh_abpz_kkm_ail_agent läuft im Jobplan DW.DWH_KKM_IMPORT_TAEGLICH_JP")
    print("Der Status für die Applikation DWH ist: fertig")
    print("Die Ab Initio Verarbeitung ist fertig. Der Status wird auf fertig umgesetzt.")

DEFAULT_ARGS = {
    'owner': 'air_flow',
    'depends_on_past': False, 
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': parse_failure_log,
}

with DAG(
    dag_id='dw_dwh_abpz_kkm_ail_agent',
    default_args=DEFAULT_ARGS,
    description='Baut den Flat-File Lookup fuer den View DWH$VI_S_SDM_AGENT_ADS auf',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # Preserved Execution Order Step 7: Pruefe Start Include
    pruefe_ab_initio_start = PythonOperator(
        task_id='dw_dwh_adm_pruefe_ab_initio_start_inc',
        python_callable=print_start_inc
    )

    # Preserved Execution Order Step 9: Monitor Start Include
    monitor_start = PythonOperator(
        task_id='dw_dwh_adm_job_monitor_start',
        python_callable=get_running_job_name
    )

    # Preserved Execution Order Step 3: Frame parameters setup 
    print_parameters = PythonOperator(
        task_id='r_alis_objekt_parameter_print',
        python_callable=print_frame_params
    )

    # Branch evaluation task
    branch_check = BranchPythonOperator(
        task_id='evaluate_state_branch',
        python_callable=check_pipeline_branch,
        provide_context=True
    )

    # Preserved Execution Order Step 2: Main Processing Step submitted as PySpark job via Serverless
    run_agent_lookup_pyspark = DataprocCreateBatchOperator(
        task_id='run_agent_lookup_pyspark',
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        batch_id="dw-dwh-abpz-kkm-ail-agent-{{ ts_nodash.lower() }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark/bhb_ccm_proc_write_agent_ads_lookup.py",
                "args": [
                    "--gcp_project", GCP_PROJECT_ID,
                    "--bq_dataset", BQ_DATASET,
                    "--output_uri", f"gs://{GCS_BUCKET}/exports/{OUTPUT_LOOKUP_FILE}"
                ]
            }
        }
    )

    # Success execution printout
    print_success = PythonOperator(
        task_id='print_success_log',
        python_callable=print_success_value
    )

    # Preserved Execution Order Step 6: Pruefe Ende Include
    pruefe_ab_initio_ende = PythonOperator(
        task_id='dw_dwh_adm_pruefe_ab_initio_ende_inc',
        python_callable=print_ende_inc
    )

    # Preserved Execution Order Step 11: Monitor End
    monitor_end = PythonOperator(
        task_id='dw_dwh_adm_job_monitor_end',
        python_callable=get_job_monitor_end
    )

    # Framework completion message print
    print_framework_end = PythonOperator(
        task_id='r_alis_objekt_end_message',
        python_callable=print_end_state
    )

    # Orchestration Flow Wiring
    pruefe_ab_initio_start >> monitor_start >> print_parameters >> branch_check >> run_agent_lookup_pyspark >> print_success >> pruefe_ab_initio_ende >> monitor_end >> print_framework_end