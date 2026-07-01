"""
Airflow DAG migration for UC4 job:
DW.BERT_AUSD_BP_TA_BCP_MSISDN

Design notes:
- Source UC4 job is a single UNIX job with no workflow container.
- Target execution is modeled as a single BigQuery SQL task.
- SQL logic is externalized to dags/sql/dw_bert_ausd_bp_ta_bcp_msisdn.sql.
- Placeholders must be replaced via Airflow Variables / CI-CD / environment config.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# ------------------------------------------------------------------------------
# Configuration helpers
# ------------------------------------------------------------------------------

def get_airflow_variable(
    key: str,
    default: Optional[str] = None,
    deserialize_json: bool = False,
) -> Any:
    """
    Safe wrapper around Airflow Variable lookup.
    """
    try:
        return Variable.get(key, default_var=default, deserialize_json=deserialize_json)
    except Exception:
        return default


def build_job_id(dag_id: str, task_id: str, run_id: str) -> str:
    """
    Build a unique BigQuery job ID.
    """
    safe_run_id = run_id.replace(":", "_").replace("+", "_").replace("/", "_")
    return f"{dag_id}_{safe_run_id}_{task_id}"


# ------------------------------------------------------------------------------
# DAG-level configuration
# ------------------------------------------------------------------------------

GCP_PROJECT_ID = get_airflow_variable("GCP_PROJECT_ID", "YOUR_GCP_PROJECT_ID")
CONN_ID_BIGQUERY = get_airflow_variable("CONN_ID_BIGQUERY", "bigquery_default")

DAG_ID = "dw_bert_ausd_bp_ta_bcp_msisdn"
TASK_ID = "bert_ausd_bp_ta_bcp_msisdn"

DEFAULT_ARGS = {
    "owner": "uc4_migration",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=0),
}

# ------------------------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------------------------

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Migration of UC4 job DW.BERT_AUSD_BP_TA_BCP_MSISDN to Airflow + BigQuery",
    schedule=None,
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    template_searchpath=[str(Path(__file__).parent / "sql")],
    tags=["uc4_migration", "bigquery", "dw_bert"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Since the SQL script utilizes scripting statements (DECLARE, procedures, BEGIN/END),
    # destinationTable and writeDisposition must not be provided in the BigQuery Job Config.
    # The actual merge and write operations are handled directly inside the SQL.
    bert_ausd_bp_ta_bcp_msisdn = BigQueryInsertJobOperator( 
        task_id=TASK_ID,
        configuration={
            "query": {
                "query": "dw_bert_ausd_bp_ta_bcp_msisdn.sql",
                "useLegacySql": False,
                "priority": "INTERACTIVE",
            }
        },
        location=get_airflow_variable("BIGQUERY_LOCATION", "EU"),
        gcp_conn_id=CONN_ID_BIGQUERY,
        job_id=build_job_id(DAG_ID, TASK_ID, "{{ run_id }}"),
        deferrable=False,
    )

    end = EmptyOperator(task_id="end")

    start >> bert_ausd_bp_ta_bcp_msisdn >> end