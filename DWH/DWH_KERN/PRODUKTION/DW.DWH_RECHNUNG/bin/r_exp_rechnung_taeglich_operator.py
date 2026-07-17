"""
Module containing reusable execution logic, date calculations, and row validations
for the daily invoice export process. Maintains structural alignment with the 
legacy shell script folder architecture.
"""

import datetime
import logging
from typing import Any, Dict, Optional
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Setup Module-level Logger
logger = logging.getLogger("airflow.task")


def resolve_stichtag(logical_date: datetime.datetime, dag_run_conf: Optional[Dict[str, Any]] = None, **kwargs: Any) -> str:
    """
    Resolves the target reference date (Stichtag) in YYYYMMDD format.
    
    Priority:
    1. 's' key passed in the manual execution configuration payload (dag_run.conf).
    2. Fallback to the logical date (yesterday relative to execution date).

    Args:
        logical_date (datetime.datetime): Airflow logical execution date.
        dag_run_conf (Optional[Dict[str, Any]]): Configuration dictionary from manual trigger.
        **kwargs: Catch-all for extra task-instance context.

    Returns:
        str: Resolved Stichtag in YYYYMMDD format.
    """
    # Legacy XML print literal
    print("Rechnungsexport fuer Stichtag...")
    
    l_stichtag = None
    if dag_run_conf and isinstance(dag_run_conf, dict):
        l_stichtag = dag_run_conf.get("s")
        
    if not l_stichtag:
        # Subtract 1 day as per legacy extraction logic matching yesterday's partition
        yesterday = logical_date - datetime.timedelta(days=1)
        l_stichtag = yesterday.strftime("%Y%m%d")
        
    # Verbatim KSH Literal print statement
    print(f"Starte Export Rechnungsdaten fuer Stichtag {l_stichtag}")
    
    # Store resolved date in context XCom for downstream usage
    ti = kwargs.get("ti")
    if ti:
        ti.xcom_push(key="l_Stichtag", value=l_stichtag)
        
    return l_stichtag


def run_count_query(gcp_conn_id: str, project_id: str, dataset_id: str, table_id: str, target_date: str) -> int:
    """
    Executes a high-performance validation query on BigQuery to compute record counts.

    Args:
        gcp_conn_id (str): Airflow Google Cloud Connection Identifier.
        project_id (str): Destination GCP Project ID.
        dataset_id (str): BigQuery Dataset ID.
        table_id (str): BigQuery Table ID.
        target_date (str): Target partition date formatted as YYYYMMDD.

    Returns:
        int: Total matched record count.
    """
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
    
    query = f"""
        SELECT COUNT(1) as total_count 
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE rechnungs_datum = PARSE_DATE('%Y%m%d', '{target_date}')
    """
    
    records = hook.get_records(sql=query)
    return int(records[0][0]) if records and len(records[0]) > 0 else 0


def validate_and_log_export(gcp_conn_id: str = "google_cloud_default", **kwargs: Any) -> int:
    """
    Validates execution results by matching target row counts and outputs logs.
    Includes fallback default properties using safe dynamic lookup values.

    Args:
        gcp_conn_id (str): Airflow Google Connection ID. Defaults to "google_cloud_default".
        **kwargs: Context dict injected by PythonOperator.

    Returns:
        int: Number of matched exported rows.
    """
    ti = kwargs["ti"]
    
    # Retrieve Resolved Stichtag from upstream context task
    l_stichtag = ti.xcom_pull(key="l_Stichtag", task_ids="resolve_stichtag")
    if not l_stichtag:
        raise ValueError("Failed to retrieve 'l_Stichtag' from the task execution workspace (XCom).")

    # Fetch configuration settings using the Environment Variable Policy
    gcp_project = Variable.get("GCP_PROJECT")
    bq_dataset = Variable.get("BQ_DATASET", default_var="dwh_kern")
    bq_table = "t_rechnung"

    l_anzahl = run_count_query(
        gcp_conn_id=gcp_conn_id,
        project_id=gcp_project,
        dataset_id=bq_dataset,
        table_id=bq_table,
        target_date=l_stichtag
    )

    # Verbatim KSH Literals
    print(f"Anzahl exportierter Rechnungssaetze: {l_anzahl}")
    
    if l_anzahl == 0:
        # Legacy error level "W" (Warning) format logged to stderr
        now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.warning(f"[W] {now_str} Keine Rechnungsdaten fuer Stichtag {l_stichtag} exportiert")
        
    print("Export Rechnungsdaten ohne erkennbare Fehler beendet")
    return l_anzahl