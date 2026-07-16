"""
Module: dw_dwh_vertrag_tarif_sync_ende.py
Purpose: Logic task representing state cleanup, lock releases and successful completion.
"""
from airflow.models import Variable
from dags.dwh_vertrag.includes.dw_hole_pfad_vtrg import get_path_variables
from dags.dwh_vertrag.includes.dw_lese_log_vtrg import write_execution_log


def execute_ende_task(**context) -> None:
    """Retrieves path configurations, releases the active sync lock back to

    'FREI', and logs a success audit trace.
    """
    # Fetch paths via include module
    paths = get_path_variables()

    # Retrieve last execution run date
    lauf_datum = Variable.get(
        "DW_VARIABLEN_VTRG_LETZTER_LAUF", default_var="UNKNOWN"
    )

    # Release transactional lock variable
    Variable.set("DW_VARIABLEN_VTRG_SYNC_STATUS", "FREI")

    # OUTPUT/PRINT LITERAL RULE: Must match German source text exactly
    print(f"Vertrags-/Tarifabgleich fuer Lauf {lauf_datum} erfolgreich beendet")

    # Log Execution metadata
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    write_execution_log(admjob=task_id, admjp=dag_id)