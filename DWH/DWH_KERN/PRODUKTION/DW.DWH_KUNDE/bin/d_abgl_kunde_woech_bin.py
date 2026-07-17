"""
Business Logic module representing the migrated KornShell (KSH) orchestration.
This script handles the execution of BigQuery queries, output formatting, 
and discrepancy reporting while preserving legacy log literals.
"""

import os
import logging
from typing import Any, Dict, List
from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models import Variable

# Setup module-level logger
logger = logging.getLogger(__name__)


def read_sql_template(sql_file_path: str) -> str:
    """Reads and returns the raw SQL template string from disk."""
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"SQL file not found at: {sql_file_path}")
    with open(sql_file_path, "r", encoding="utf-8") as f:
        return f.read()


def get_bigquery_client(gcp_project: str, connection_id: str = 'google_cloud_default') -> bigquery.Client:
    """Acquires a BigQuery Client instance via Airflow's BigQueryHook."""
    hook = BigQueryHook(gcp_conn_id=connection_id, use_legacy_sql=False)
    return hook.get_client(project_id=gcp_project)


def execute_comparison_query(
    client: bigquery.Client, 
    sql_query: str, 
    stichtag: str
) -> List[bigquery.Row]:
    """Configures and runs the BQ query job, returning the resulting rows."""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("p_Stichtag", "STRING", stichtag)
        ]
    )
    query_job = client.query(sql_query, job_config=job_config)
    results = query_job.result()
    return list(results)


def execute_reconciliation_logic(context: Dict[str, Any]) -> None:
    """
    Orchestrates the entire reconciliation check.
    Fulfills Output Literal Preservation and logging specifications.
    """
    # 1. Environment Variable Retrieval (Strict Policy)
    gcp_project = os.environ.get("GCP_PROJECT")
    if not gcp_project:
        raise ValueError("GCP_PROJECT environment variable is not set.")
    
    # 2. Extract Execution Metadata
    l_Stichtag = context['ds_nodash']
    lauf_woche = context['dag_run'].run_id if context.get('dag_run') else "weekly_run"
    
    # 3. VERBATIM Legacy Output Logs
    # Literal 1: XML trigger start
    print(f"Kundenadressabgleich fuer Lauf {lauf_woche} angestossen")
    
    # Literal 2: KSH validation start
    print(f"Starte Adressabgleich Kundenstammdaten fuer Stichtag {l_Stichtag}")
    
    # Resolve SQL script location relative to this file
    bin_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(os.path.dirname(bin_dir), "sql", "d_abgl_kunde_woech.sql")
    
    try:
        # Load and execute Query
        sql_template = read_sql_template(sql_file_path)
        bq_client = get_bigquery_client(gcp_project=gcp_project)
        rows = execute_comparison_query(bq_client, sql_template, l_Stichtag)
        
        l_Abweichungen = len(rows)
        l_Protokoll_Datei = f"gs://{Variable.get('GCS_BUCKET')}/logs/{l_Stichtag}_d_abgl_kunde_woech.log"
        
        # Literal 3: Discrepancy counter message
        print(f"Anzahl gefundener Abweichungen: {l_Abweichungen}")
        
        if l_Abweichungen > 0:
            # Literal 5: Warning / Alert message
            print(f"{l_Abweichungen} Abweichungen im Kundenadressabgleich gefunden, siehe {l_Protokoll_Datei}")
            
            # Print tabular records to execution log
            for row in rows:
                print(
                    f"MARKER={row.MARKER}, KUNDE={row.KUNDE}, NACHNAME={row.NACHNAME}, VORNAME={row.VORNAME}, "
                    f"PLZ={row.PLZ} vs REF_PLZ={row.REF_PLZ}, ORT={row.ORT} vs REF_ORT={row.REF_ORT}, "
                    f"STRASSE={row.STRASSE} vs REF_STRASSE={row.REF_STRASSE}"
                )
        
        # Literal 4: Finished with zero errors message
        print("Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet")
        
    except Exception as e:
        logger.error("Error occurred during customer data reconciliation execution.")
        raise e