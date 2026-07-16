"""
DAG: dw_dwh_stamm_knzb_abgl_jp
Purpose: Orchestrates daily reconciliation of customer master relationship data.
Source Reference: DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/DW.DWH_STAMM_KNZB_ABGL_JP.xml
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Import modular helper functions adhering to the Folder Integrity Rule
from includes.dw_hole_pfad_knzb import resolve_paths
from includes.dw_lese_log_knzb import log_activity

# ── GLOBAL ENV CONFIGURATION ──────────────────────────────────────────────────
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=None)

# ── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 4),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_stamm_knzb_abgl_jp",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Daily execution at 06:00 UTC
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md="""
    ### Daily Master Data Reconciliation KNZB
    Reconciles Kundennummer/Basiszugangs-Stammdaten (KNZB) master data from the 
    source systems to the DWH Core Layer.
    Converted from UC4 Job Plan: `DW.DWH_STAMM_KNZB_ABGL_JP`
    """,
) as dag:

    # ── Task 1: Start JS Logic ────────────────────────────────────────────────
    def run_start_js(**context: Any) -> None:
        """Executes the startup checks and sets active lock states.

        Formerly mapped to: DW.DWH_STAMM_KNZB_ABGL_START_JS
        """
        # 1. Resolve path variables (From separated include module)
        job_config = resolve_paths()

        # 2. Extract execution timestamp context
        lauf_datum = datetime.now().strftime("%Y%m%dd")
        execution_date_str = context["ds"]

        # 3. Check status locking variable
        abgleich_status = Variable.get(
            "dw_variablen_knzb_abgleich_status", default_var="FREI"
        )

        if abgleich_status == "GESPERRT":
            # OUTPUT/PRINT LITERAL RULE: Verbatim preservation of German log/abort message
            error_msg = f"KNZB-Abgleich fuer {lauf_datum} ist gesperrt - Abbruch der Verarbeitung"
            print(error_msg)
            raise AirflowFailException(f"Aborted: {error_msg}")

        # 4. Set state tracking variables to active lock state
        Variable.set("dw_variablen_knzb_abgleich_status", "LAEUFT")
        Variable.set("dw_variablen_knzb_letzter_lauf", execution_date_str)

        # 5. Log activity (From separated include module)
        log_activity(context["dag"].dag_id, context["task"].task_id)

    task_start_js = PythonOperator(
        task_id="dw_dwh_stamm_knzb_abgl_start_js",
        python_callable=run_start_js,
        provide_context=True,
    )

    # ── Task 2: Ende JS Logic ─────────────────────────────────────────────────
    def run_ende_js(**context: Any) -> None:
        """Releases active lock states and logs successful pipeline completion.

        Formerly mapped to: DW.DWH_STAMM_KNZB_ABGL_ENDE_JS
        """
        # 1. Resolve path variables (From separated include module)
        job_config = resolve_paths()

        # 2. Retrieve last execution timestamp from global state
        lauf_datum = Variable.get(
            "dw_variablen_knzb_letzter_lauf",
            default_var=datetime.now().strftime("%Y%m%d"),
        )

        # 3. Release status variable lock to allow future runs
        Variable.set("dw_variablen_knzb_abgleich_status", "FREI")

        # 4. Success Completion printing (OUTPUT/PRINT LITERAL RULE)
        print(
            f"KNZB-Stammdatenabgleich fuer Lauf {lauf_datum} erfolgreich beendet"
        )

        # 5. Log activity (From separated include module)
        log_activity(context["dag"].dag_id, context["task"].task_id)

    task_ende_js = PythonOperator(
        task_id="dw_dwh_stamm_knzb_abgl_ende_js",
        python_callable=run_ende_js,
        provide_context=True,
    )

    # ── Execution Dependency Map ──────────────────────────────────────────────
    task_start_js >> task_ende_js