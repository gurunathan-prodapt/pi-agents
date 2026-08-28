from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

# ==============================================================================
# ── GLOBAL CONFIGURATION ──────────────────────────────────────────────────────
# ==============================================================================
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DWH_HOME_PATH = Variable.get("DWH_HOME_PATH")

# ==============================================================================
# ── DEFAULT ARGS ──────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# ── DAG DEFINITION ────────────────────────────────────────────────────────────
# ==============================================================================
with DAG(
    dag_id="dw_bert_p_rech_empf",
    default_args=DEFAULT_ARGS,
    description="BERT_P_RECH_EMPF: Aufbereitung der Rechnungsempfänger",
    schedule=None,  # Handled externally / triggered on demand
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,  # Strict concurrency limit based on sync requirements
    is_paused_upon_creation=False,
    tags=["dwh", "uc4_migration"],
    doc_md="""
### BERT_P_RECH_EMPF: Aufbereitung der Rechnungsempfänger
Restart jederzeit möglich.
Synchronisation gegen DW.BERT_STAMMDATEN

erwartete Laufzeit 1:30h
TEst 1:15h
"""
) as dag:

    # ==========================================================================
    # ── Upstream Synchronization ──────────────────────────────────────────────
    # ==========================================================================
    # Watch the completion of the DW_BERT_STAMMDATEN DAG
    wait_for_stammdaten = ExternalTaskSensor(
        task_id="wait_for_stammdaten",
        external_dag_id="dw_bert_stammdaten",
        external_task_id=None,  # waits for the entire DAG to complete
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke",
        poke_interval=60,
        timeout=7200,  # 2 hours timeout
    )

    # ==========================================================================
    # ── Task: dw_bert_p_rech_empf_task ────────────────────────────────────────
    # ==========================================================================
    # Executes the migrated Python script r_ausd_rechempf.py
    # Environment variable DWH_JOB_KENNUNG is set to 'BERT_P_RECH_EMPF'
    # Pool 'DW_BERT_RECH_SYNC' enforces mutual exclusion
    dw_bert_p_rech_empf_task = BashOperator(
        task_id="dw_bert_p_rech_empf_task",
        bash_command=f"python {DWH_HOME_PATH}/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.py",
        env={
            "DWH_JOB_KENNUNG": "BERT_P_RECH_EMPF",
            "GCP_PROJECT": GCP_PROJECT,
            "GCP_REGION": GCP_REGION,
        },
        pool="DW_BERT_RECH_SYNC",
        execution_timeout=timedelta(hours=2),
    )

    # ==========================================================================
    # ── Dependencies ──────────────────────────────────────────────────────────
    # ==========================================================================
    wait_for_stammdaten >> dw_bert_p_rech_empf_task