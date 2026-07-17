"""
Module: r_abgl_kunde_woech.py
Description: Translates legacy shell wrapper logic (r_abgl_kunde_woech.ksh) 
             to native Python execution using the Google Cloud BigQuery client.
"""

import sys
import os
import datetime
import logging
from google.cloud import bigquery

# Configure Logger to output directly to standard stdout/stderr for Composer logs
logger = logging.getLogger("airflow.task")


def resolve_stichtag(stichtag: str = None) -> str:
    """
    Resolves the execution date parameter. 
    If no date is provided, defaults to the date 7 days ago (YYYYMMDD).
    """
    if not stichtag:
        seven_days_ago = datetime.date.today() - datetime.timedelta(days=7)
        return seven_days_ago.strftime('%Y%m%d')
    return stichtag


def log_start_reconciliation(stichtag: str) -> None:
    """
    Prints legacy start execution logs verbatim in original German.
    """
    # XML Print Statement (Verbatim)
    print(f"Kundenadressabgleich fuer Lauf {stichtag} angestossen")
    # KSH Print Statement (Verbatim)
    print(f"Starte Adressabgleich Kundenstammdaten fuer Stichtag {stichtag}")


def execute_reconciliation_query(
    client: bigquery.Client, 
    project_id: str, 
    dataset_dwh: str, 
    dataset_stamm: str, 
    stichtag: str
) -> list:
    """
    Generates and executes the SQL query logic matching d_abgl_kunde_woech.sql.
    """
    sql_path = os.path.join(os.path.dirname(__file__), "../sql/d_abgl_kunde_woech.sql")
    
    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            query = f.read()
        # Standardize parameter variables in standard BigQuery SQL format
        query = query.replace("@stichtag", f