from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable

# Retrieve SSH Connection ID from Airflow Variables
SSH_CONN_ID = Variable.get("SSH_CONN_DWHDWH1P", default_var="ssh_dwhdwh1p_default")

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_dwh_exis_cpdw_loc",
    default_args=DEFAULT_ARGS,
    schedule=None,  # Standalone DAG, externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["uc4_migration", "standalone_jobs"],
    doc_md="""
### UC4 Job Migration: DW.DWH_EXIS_CPDW_LOC
**Description:** Exportiert Lookupdaten nach CPDW
**Documentation:** Wiederanlauf ohne weitere Maßnahmen möglich etc.
""",
) as dag:

    # Task to execute the export command on the remote host via SSH
    dwh_exis_cpdw_loc = SSHOperator(
        task_id="dwh_exis_cpdw_loc",
        ssh_conn_id=SSH_CONN_ID,
        command="source $HOME/.dw_init && export DWH_JOB_KENNUNG='EXIS_CPDW_LOC' && $HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r loc -f sftp",
        environment={"DWH_JOB_KENNUNG": "EXIS_CPDW_LOC"},
    )

    dwh_exis_cpdw_loc