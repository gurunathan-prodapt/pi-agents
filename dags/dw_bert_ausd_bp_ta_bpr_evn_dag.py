"""
Modular BigQuery/Airflow implementation for the migrated workload:
dw_bert_ausd_bp_ta_bpr_evn

This file is designed to be reusable and easy to adapt for Composer.
It includes:
- SQL builders
- parameter parsing/validation
- DAG factory
- BigQuery task factory

Adjust project/dataset/table names as needed.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

DEFAULT_PROJECT_ID = "gcp-bert-prd"
DEFAULT_DATASET = "bert_dataset"
DEFAULT_LOCATION = "EU"
DEFAULT_CONN_ID = "google_cloud_default"

DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "airflow-bert",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 21),
    "email_on_failure": True,
    "email": ["dw_alert_production@company.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

EVN_BPR_IDS: List[int] = [32, 2506, 2839, 2840, 3055, 3056, 3821]


# -----------------------------------------------------------------------------
# Utility helpers
# -----------------------------------------------------------------------------

def fq_table(project_id: str, dataset: str, table: str) -> str:
    """Return a fully-qualified BigQuery table reference."""
    return f"`{project_id}.{dataset}.{table}`"


def normalize_date_yyyymmdd(value: str) -> str:
    """
    Validate and normalize a date string in YYYYMMDD format.
    Returns the same string if valid.
    """
    if not isinstance(value, str):
        raise ValueError("stichtag must be a string in YYYYMMDD format")
    if len(value) != 8 or not value.isdigit():
        raise ValueError("stichtag must be in YYYYMMDD format")
    datetime.strptime(value, "%Y%m%d")
    return value


def parse_int(value: Any, default: int = 0) -> int:
    """Safely parse an integer-like value."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer value: {value}") from exc


def get_conf_value(context: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Read a value from dag_run.conf safely."""
    dag_run = context.get("dag_run")
    if dag_run and getattr(dag_run, "conf", None):
        return dag_run.conf.get(key, default)
    return default


# -----------------------------------------------------------------------------
# SQL builders
# -----------------------------------------------------------------------------

def build_evn_sql(
    project_id: str = DEFAULT_PROJECT_ID,
    dataset: str = DEFAULT_DATASET,
    source_table: str = "sof_ta_bpr_instance",
    target_table: str = "sof_ta_bpr_evn",
    bpr_ids: Optional[Iterable[int]] = None,
) -> str:
    """
    Build the BigQuery SQL for the EVN basis product load.

    Logic:
    - truncate target table
    - insert filtered rows from source table
    """
    ids = list(bpr_ids) if bpr_ids is not None else EVN_BPR_IDS
    ids_sql = ",\n        ".join(str(x) for x in ids)

    src = fq_table(project_id, dataset, source_table)
    tgt = fq_table(project_id, dataset, target_table)

    return f"""
    -- Step 1: Truncate Target Table
    TRUNCATE TABLE {tgt};

    -- Step 2: Insert Filtered EVN Basis Product Instances
    INSERT INTO {tgt} (cntrct_id, bpr_id)
    SELECT
        bp.cntrct_id,
        bp.bpr_id
    FROM {src} AS bp
    WHERE bp.bpr_id IN (
        {ids_sql}
    );
    """.strip()


# -----------------------------------------------------------------------------
# Airflow callables
# -----------------------------------------------------------------------------

def prepare_evn_sql(**context) -> str:
    """
    Build SQL dynamically from DAG run configuration.
    Expected conf keys:
      - project_id
      - dataset
      - source_table
      - target_table
      - bpr_ids
    """
    project_id = get_conf_value(context, "project_id", DEFAULT_PROJECT_ID)
    dataset = get_conf_value(context, "dataset", DEFAULT_DATASET)
    source_table = get_conf_value(context, "source_table", "sof_ta_bpr_instance")
    target_table = get_conf_value(context, "target_table", "sof_ta_bpr_evn")
    bpr_ids = get_conf_value(context, "bpr_ids", EVN_BPR_IDS)

    if isinstance(bpr_ids, str):
        bpr_ids = [parse_int(x.strip()) for x in bpr_ids.split(",") if x.strip()]
    else:
        bpr_ids = [parse_int(x) for x in bpr_ids]

    return build_evn_sql(
        project_id=project_id,
        dataset=dataset,
        source_table=source_table,
        target_table=target_table,
        bpr_ids=bpr_ids,
    )


def validate_runtime_params(**context) -> Dict[str, Any]:
    """
    Validate runtime parameters and return normalized values.
    Useful if you later extend the DAG with restart/date-based logic.
    """
    stichtag = get_conf_value(context, "stichtag", None)
    wiederanlauf_wert = parse_int(get_conf_value(context, "wiederanlauf_wert", 0), 0)

    result: Dict[str, Any] = {"wiederanlauf_wert": wiederanlauf_wert}
    if stichtag is not None:
        result["stichtag"] = normalize_date_yyyymmdd(stichtag)
    return result


# -----------------------------------------------------------------------------
# DAG factory
# -----------------------------------------------------------------------------

def create_dag(
    dag_id: str = "dw_bert_ausd_bp_ta_bpr_evn",
    default_args: Optional[Dict[str, Any]] = None,
) -> DAG:
    """Create the Airflow DAG."""
    dag_default_args = default_args or DEFAULT_ARGS

    dag = DAG(
        dag_id=dag_id,
        default_args=dag_default_args,
        description="Orchestration DAG to provision and filter EVN Basisproduct data inside BigQuery",
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
        tags=["bigquery", "bert", "basisprodukt", "evn"],
    )

    with dag:
        validate_task = PythonOperator(
            task_id="validate_runtime_params",
            python_callable=validate_runtime_params,
        )

        prepare_sql_task = PythonOperator(
            task_id="prepare_evn_sql",
            python_callable=prepare_evn_sql,
        )

        run_evn_provisioning = BigQueryExecuteQueryOperator(
            task_id="run_evn_provisioning",
            sql="{{ ti.xcom_pull(task_ids='prepare_evn_sql') }}",
            use_legacy_sql=False,
            location=DEFAULT_LOCATION,
            gcp_conn_id=DEFAULT_CONN_ID,
        )

        validate_task >> prepare_sql_task >> run_evn_provisioning

    return dag


# -----------------------------------------------------------------------------
# DAG instance
# -----------------------------------------------------------------------------

dag = create_dag()