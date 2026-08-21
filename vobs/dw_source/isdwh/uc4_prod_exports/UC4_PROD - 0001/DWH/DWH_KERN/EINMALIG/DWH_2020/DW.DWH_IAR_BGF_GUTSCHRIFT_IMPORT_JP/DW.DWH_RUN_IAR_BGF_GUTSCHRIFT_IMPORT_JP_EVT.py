from datetime import datetime, timedelta
import fnmatch
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook

# ==============================================================================
# GLOBAL CONFIGURATION
# ==============================================================================
# Sourced at runtime via Airflow Variables
SFTP_CONN_ID = Variable.get("SFTP_CONN_ID", default_var="sftp_default")
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None)

# ==============================================================================
# JOB-SPECIFIC CONFIGURATION
# ==============================================================================
FILE_PATH = "/app_dwh/sftp_users/istcomis/daten/tcom/iar/work/DWHK_DWHM_IAR_GUTSCHR_*.chk"
TARGET_DAG_ID = "dw_dwh_iar_bgf_gutschrift_import_jp"

# ==============================================================================
# ON FAILURE CALLBACKS
# ==============================================================================
def on_failure_alarm(context):
    # Original UC4 behaviour called: DW.CALL_STANDARD
    # Sourced via the Airflow on-failure alert mechanism
    print("Activating standard call alert (DW.CALL_STANDARD)...")

# ==============================================================================
# CUSTOM SFTP WILDCARD SENSOR
# ==============================================================================
class SFTPWildcardSensor(BaseSensorOperator):
    template_fields = ("path",)
    
    def __init__(self, path, sftp_conn_id="sftp_default", **kwargs):
        super().__init__(**kwargs)
        self.path = path
        self.sftp_conn_id = sftp_conn_id

    def poke(self, context):
        hook = SFTPHook(ssh_conn_id=self.sftp_conn_id)
        directory = os.path.dirname(self.path)
        pattern = os.path.basename(self.path)
        
        try:
            files = hook.list_directory(directory)
            matching_files = [f for f in files if fnmatch.fnmatch(f, pattern)]
            if matching_files:
                self.log.info(f"Found matching files: {matching_files}")
                return True
        except Exception as e:
            self.log.warning(f"Error checking directory {directory}: {e}")
        
        return False

# ==============================================================================
# DOWNSTREAM TRIGGER LOGIC
# ==============================================================================
def trigger_downstream_logic(**context):
    from airflow.models import DagRun
    from airflow.utils.state import State
    from airflow.api.common.trigger_dag import trigger_dag
    
    active_runs = DagRun.find(dag_id=TARGET_DAG_ID, state=State.RUNNING)
    
    if len(active_runs) > 0:
        # Match the exact print statement from source: "Jobplan &StartJp is active!"
        print(f"Jobplan {TARGET_DAG_ID} is active!")
    else:
        # Match the exact print statement from source: "Starting Jobplan &StartJp ..."
        print(f"Starting Jobplan {TARGET_DAG_ID} ...")
        try:
            trigger_dag(dag_id=TARGET_DAG_ID, run_id=f"triggered__{TARGET_DAG_ID}__{context['ds_nodash']}")
            # Match the exact print statement from source: "JP started at &date ..."
            date_str = datetime.now().strftime('%Y%m%d')
            print(f"JP started at {date_str} ...")
        except Exception as e:
            # If activation fails (equivalent to AKTOBJ = "0000000")
            # Trigger standard call/alert
            on_failure_alarm(context)
            raise e

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
with DAG(
    dag_id="dw_dwh_run_iar_bgf_gutschrift_import_jp_evt",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure_alarm,
    },
    description="File event sensor DAG for IAR BGF Gutschrift Import",
    schedule=None,  # Event-driven execution
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["uc4_migration", "event_file"],
) as dag:

    # detect_file_event: File monitoring task
    detect_file_event = SFTPWildcardSensor(
        task_id="detect_file_event",
        path=FILE_PATH,
        sftp_conn_id=SFTP_CONN_ID,
        poke_interval=300,
        timeout=3600,
        mode="reschedule",
    )

    # trigger_downstream_dag: Activates the downstream workflow
    trigger_downstream_dag = PythonOperator(
        task_id="trigger_downstream_dag",
        python_callable=trigger_downstream_logic,
    )

    # ==========================================================================
    # DEPENDENCIES
    # ==========================================================================
    detect_file_event >> trigger_downstream_dag