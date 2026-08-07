"""
DAG: dw_dwh_dummy_absd_plato_tarife
Description: Converted from UC4 JOBS_UNIX object DW.DWH_DUMMY_ABSD_PLATO_TARIFE.
This DAG represents a dummy execution placeholder for downstream mapping processes.

Legacy Metadata & Operational Notes:
- German Operational Note: "Wiederanlauf ohne weitere Maßnahmen möglich" (Restart is possible without further actions)
- Legacy Host: |DWHDWH1P|HOST
- Legacy Login: DW.UNIX.ISTNS (maps to Google Cloud Service Account in Cloud Composer GKE)
- Original UC4 command: :print Doing nothinig
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=DEFAULT_ARGS,
    description="Converted from UC4 JOBS_UNIX object DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ─── TASK: dwh_dummy_absd_plato_tarife ────────────────────────────────────
    # Converted dummy synchronization point.
    # Original command: :print Doing nothinig
    dwh_dummy_absd_plato_tarife = BashOperator(
        task_id="dwh_dummy_absd_plato_tarife",
        bash_command="echo 'Doing nothinig'",
    )

    # ─── DEPENDENCIES ─────────────────────────────────────────────────────────
    # Standalone dummy task; no upstream or downstream dependencies defined in this extraction.
    dwh_dummy_absd_plato_tarife