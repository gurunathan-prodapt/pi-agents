"""
Module: dw_dwh_vertrag_tarif_sync_start.py
Purpose: Logic task representing start initialization and safety lock verification.
"""
from datetime import datetime

from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from dags.dwh_vertrag.includes.dw_hole_pfad_vtrg import get_path_variables
from dags.dwh_vertrag.includes.dw_lese_log_vtrg import write_execution_log


def execute_start_task(**context) -> None:
    """Retrieves path configurations, validates that the workflow status is not

    locked ('GESPERRT'), sets state tracking variables, and logs task metadata.
    """
    # Fetch paths via include module
    paths = get_path_variables()

    # Define operational parameters
    lauf_datum = datetime.now().strftime("%Y%m%d")

    # Read current state container variable
    sync_status = Variable.get("DW_VARIABLEN_VTRG_SYNC_STATUS", default_var="FREI")

    # Lock state boundary validation
    if sync_status == "GESPERRT":
        # OUTPUT/PRINT LITERAL RULE: Must match German source text exactly
        print(f"Vertrags-/Tarifabgleich fuer {lauf_datum} ist gesperrt - Abbruch")
        raise AirflowFailException("Job aborted due to GESPERRT status lock.")

    # Mutate status to active ('LAEUFT') and update last run date
    Variable.set("DW_VARIABLEN_VTRG_SYNC_STATUS", "LAEUFT")
    Variable.set("DW_VARIABLEN_VTRG_LETZTER_LAUF", lauf_datum)

    # Log Execution metadata
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    write_execution_log(admjob=task_id, admjp=dag_id)