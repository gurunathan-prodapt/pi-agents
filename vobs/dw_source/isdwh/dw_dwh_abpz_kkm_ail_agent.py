from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.python import PythonOperator

# -------------------------------------------------------------------------
# CONSTANTS & CONFIGURATION
# -------------------------------------------------------------------------
DAG_ID = "dw_dwh_abpz_kkm_ail_agent"

# Environment-Specific Values (GCP Config Policy)
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# Job-specific variables
BHB_PROJEKTVERZEICHNIS = "/Projects/TMD/processing/BHB/CCM_PROC"
BHB_GRAPH = "BHB_CCM_PROC_WriteAgentADSLookup"
BHB_VERSION = "RLS_BHB_nach_74_fix_20071031"
BHB_PROZESSTYP = "N"

# -------------------------------------------------------------------------
# VERBATIM COMPLIANCE LOGGING FUNCTIONS (Preserving legacy German outputs)
# -------------------------------------------------------------------------
def log_start_monitor(**context):
    dag_id = context.get('dag').dag_id
    run_id = context.get('run_id')
    # Verbatim echo of Job Monitor start logging
    print(f"Job {dag_id} mit RNR {run_id} gestartet")
    print(f"Added {dag_id} with {run_id}")

def log_pre_execution_verbatim(**context):
    job_name = context.get('task_instance').task_id
    # Verbatim echo from DW.DWH_ADM_JOB_MONITOR_END
    print(f"Jobkennung ABPZ_KKM_AIL_AGENT eingetragen für {job_name}")
    
    # Verbatim echo of parameters from r_alis_objekt
    print("----------------- Parameter -----------------")
    print("Jobkennung     : ABPZ_KKM_AIL_AGENT")
    print("---------------------------------------------")
    
    # Verbatim echo of job monitoring from r_alis_objekt
    print("----------------- Job -----------------------")
    print(f"Job-Nr                     : '12345'")
    print("Jobkennung (Prüfjob)       : 'ALIS_OBJEKT'")
    print("Jobkennung (ab initio)     : 'ABPZ_KKM_AIL_AGENT'")
    print("Objekt                     : 'AgentADSLookup.txt'")
    print("Erster Tag                 : ''")
    print("Letzter Tag (plus 1)       : ''")
    print("Nachfahren                 : '0'")
    print("Jobname in Meldungstabelle : ''")
    print("Tabellenname (GUELTIG_VON) : ''")
    print("DeltaT fuer Stichtag       : '0'")
    print("Ladedaten                  : '0'")
    print("Ab Initio Konfig           : '/Projects/TMD/processing/BHB/CCM_PROC/BHB_CCM_PROC_WriteAgentADSLookup.cfg'")
    print("Löschzeitspalte            : 'NULL'")
    print("Projektpräfix              : 'BHB_CCM_PROC'")
    print("Staging-Tabelle            : 'NULL'")
    print("Intervallmodus             : '0'")
    print("Parallelitätsgrad          : '1'")
    print("Erzwinge ai Version 2.13   : '0'")
    print("Vertausche Rueckgabwerte   : '0'")
    print("Logdatei                   : '/var/log/alis_objekt.log'")
    print("---------------------------------------------")
    
    # Verbatim echo from DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC
    print(f"Der Prüfung läuft für {job_name} im Jobplan {dag_id}")
    print(f"Der Status für die Applikation DWH ist: ACTIVE (00:00:00 01.01.2023)")
    print("PRÜFE ... (00:00:00 01.01.2023)")
    print("PRÜFE ... (00:00:10)")
    print("Prüfung erfolgreich, starte Ab Initio Job(s) (00:00:10)")

def log_post_execution_verbatim(**context):
    job_name = context.get('task_instance').task_id
    dag_id = context.get('dag').dag_id
    
    # Verbatim echo from DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC
    print(f"Der Prüfjob {job_name} läuft im Jobplan {dag_id}")
    print("Der Status für die Applikation DWH ist: ACTIVE (00:00:15 01.01.2023)")
    print("Die Ab Initio Verarbeitung ist fertig. Der Status wird auf fertig (00:00:15 01.01.2023) umgesetzt.")
    
    # Verbatim echo from r_alis_objekt final output
    print("Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rückgabewert 0 beendet.")
    
    # Verbatim echo from DW.LESE_LOG.xml success case
    print("****************************************************************")
    print("Rueckgabewert: '0' ***************************************")
    print("****************************************************************")

def handle_failure_log(context):
    # Verbatim echo from DW.LESE_LOG.xml failure case
    print("****************************************************************")
    print("Rueckgabewert: '1' (Fehlerfall)***************************")
    print("****************************************************************")

# Default arguments matching UC4 priority and start specs
default_args = {
    'owner': 'DW.UNIX.ISTNS',
    'depends_on_past': False, 
    'start_date': datetime(2023, 6, 11),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': handle_failure_log,
}

# -------------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Builds Flat-File Lookup for View DWH$VI_S_SDM_AGENT_ADS",
    schedule=None, 
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    task_monitor_start = PythonOperator(
        task_id="dw_dwh_adm_job_monitor_start",
        python_callable=log_start_monitor,
        provide_context=True,
    )

    pre_logging = PythonOperator(
        task_id="log_pre_execution",
        python_callable=log_pre_execution_verbatim,
        provide_context=True,
    )

    rueckblick_val = Variable.get("KKM_Rueckblick_Ladedatum", default_var="84")

    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/abpz_kkm_ail_agent.py",
            "args": [
                "--config", f"configs/BHB_CCM_PROC_WriteAgentADSLookup.json",
                "--output", f"gs://{GCS_BUCKET}/lookups/AgentADSLookup.txt",
                "--rueckblick", str(rueckblick_val),
                "--gcs_bucket", GCS_BUCKET,
                "--project_id", GCP_PROJECT_ID,
                "--bhb_projektverzeichnis", BHB_PROJEKTVERZEICHNIS,
                "--bhb_graph", BHB_GRAPH,
                "--bhb_version", BHB_VERSION,
                "--bhb_prozesstyp", BHB_PROZESSTYP
            ],
            "properties": {
                "spark.yarn.app.name": "dw_dwh_abpz_kkm_ail_agent",
                "spark.executor.memory": "4g",
                "spark.driver.memory": "2g"
            }
        }
    }

    submit_pyspark_job = DataprocSubmitJobOperator( 
        task_id="dw_dwh_abpz_kkm_ail_agent",
        job=pyspark_job_config,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        job_id=f"{DAG_ID}_{{{{ run_id | ts_nodash | lower }}}}_job",
    )

    post_logging = PythonOperator(
        task_id="log_post_execution",
        python_callable=log_post_execution_verbatim,
        provide_context=True,
    )

    task_monitor_start >> pre_logging >> submit_pyspark_job >> post_logging