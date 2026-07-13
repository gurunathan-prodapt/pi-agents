from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

# ── GCP Configuration ────────────────────────────────────
# Placeholder variables (Required by framework blueprint)
YOUR_GCP_PROJECT_ID = "your-gcp-project-id"
YOUR_DATAPROC_REGION = "your-dataproc-region"
YOUR_DATAPROC_CLUSTER_NAME = "your-dataproc-cluster-name"
YOUR_BUCKET_NAME = "your-bucket-name"

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ── Status Checking Logic ──────────────────────────────
def check_ab_initio_status(**context):
    """
    Implements UC4 logic:
    - Checks the variable DW.ADM_AB_INITIO_VAR for STATUS_DWH.
    - If status is 'go', return True (sensor succeeds).
    - If status is 'exit1', raise AirflowFailException (sensor aborts immediately).
    - Otherwise, return False (sensor pokes again).
    """
    # Retrieve the Airflow variable dictionary (safely falls back to empty dict if missing)
    var_dict = Variable.get("dw_adm_ab_initio_var", deserialize_json=True, default_var={})
    
    # Get the state of the application (Defaulting to 'wait' if not found)
    app_status = var_dict.get("STATUS_DWH", "wait")
    
    print(f"Checking status for Application: DWH. Current Status: {app_status}")
    
    if app_status == "go":
        print("Status verification successful! Initiating downstream execution pipeline.")
        return True
    elif app_status == "exit1":
        raise AirflowFailException("Terminal status 'exit1' encountered. Aborting execution process.")
    
    # Returning False triggers a retry after the poke_interval expires
    return False

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_adm_pruefe_ab_initio_start_inc",
    default_args=DEFAULT_ARGS,
    schedule=None,  # Typically triggered inside a parent DAG execution
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    description="Gatekeeper sensor DAG mimicking UC4 JOBI status monitoring"
) as dag:

    # ── Sensor Task ────────────────────────────────────────
    poll_ab_initio_status = PythonSensor(
        task_id="poll_ab_initio_status",
        python_callable=check_ab_initio_status,
        poke_interval=10,      # Maps to UC4 &WAIT = '10'
        timeout=3600,          # 1-hour safety timeout limit to prevent infinite loops
        mode="poke"
    )

    # ── Dependencies ─────────────────────────────────────────
    poll_ab_initio_status