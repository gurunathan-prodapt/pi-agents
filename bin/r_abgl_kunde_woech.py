#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module: r_abgl_kunde_woech.py
Path: bin/r_abgl_kunde_woech.py

Migrated Python implementation replacing the legacy bin/r_abgl_kunde_woech.ksh.
Responsible for reading the validation SQL query, replacing placeholder tokens,
running the query against Google BigQuery, and enforcing the legacy log format.
"""

import os
import datetime
import logging
from typing import Optional
from google.cloud import bigquery

# Configure local logger for execution traceability
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def read_sql_template(sql_path: str) -> str:
    """
    Reads the SQL query script template from GCS or local disk.
    
    :param sql_path: Absolute or relative path to the SQL script.
    :return: Query content as a string.
    """
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQL file template not found at: {sql_path}")
    
    with open(sql_path, "r", encoding="utf-8") as f:
        return f.read()


def format_validation_query(
    sql_template: str, 
    gcp_project: str, 
    bq_dataset: str, 
    stichtag: str
) -> str:
    """
    Prepares the BigQuery SQL string by replacing placeholder tokens.
    
    :param sql_template: Raw SQL query containing variables or placeholders.
    :param gcp_project: Target GCP Project ID.
    :param bq_dataset: Target BigQuery Dataset.
    :param stichtag: Execution target reporting date (YYYYMMDD).
    :return: Executable BigQuery SQL query.
    """
    # Replace metadata configuration parameters
    templated_sql = sql_template.replace("@gcp_project", gcp_project)
    templated_sql = templated_sql.replace("@bq_dataset", bq_dataset)
    
    # Safely replace parameter marker with literal formatted string to prevent parameter type issues
    templated_sql = templated_sql.replace("@stichtag", f"'{stichtag}'")
    
    return templated_sql


def run_reconciliation(
    gcp_project: str,
    bq_dataset: str,
    l_stichtag: str,
    run_id: str,
    lauf_woche: str,
    sql_path: Optional[str] = None
) -> None:
    """
    Performs the core weekly address reconciliation business process.
    
    :param gcp_project: Google Cloud Project ID.
    :param bq_dataset: Target dataset containing tables.
    :param l_stichtag: Stichtag date parameter (YYYYMMDD).
    :param run_id: Orchestrator execution identification tag.
    :param lauf_woche: Airflow execution partition date context.
    :param sql_path: Optional path to external .sql template query file.
    """
    # 1. CHARACTER-FOR-CHARACTER LOG PRESERVATION (Start Message)
    print(f"Starte Adressabgleich Kundenstammdaten fuer Stichtag {l_stichtag}")
    
    # Load and render query string
    if sql_path:
        try:
            sql_template = read_sql_template(sql_path)
            query_string = format_validation_query(sql_template, gcp_project, bq_dataset, l_stichtag)
        except Exception as e:
            logger.warning(f"Could not load SQL file. Falling back to inline SQL definition. Error: {e}")
            sql_path = None

    if not sql_path:
        # Fallback inline SQL (fully matching converted architecture)
        query_string = f"""
        SELECT 
          CASE 
            WHEN src.adresse != ref.adresse THEN CONCAT('ABWEICHUNG: Kunde ', src.kunden_id, ' hat abweichende Adresse.')
            ELSE 'OK'
          END AS status_msg
        FROM 
          `{gcp_project}.{bq_dataset}.kunde_stammdaten` AS src
        LEFT JOIN 
          `{gcp_project}.{bq_dataset}.referenz_stammdaten` AS ref
        ON 
          src.kunden_id = ref.kunden_id
        WHERE 
          src.stichtag = '{l_stichtag}';
        """

    # Initialize Google Cloud BigQuery client
    client = bigquery.Client(project=gcp_project)
    
    try:
        query_job = client.query(query_string)
        results = query_job.result()
    except Exception as e:
        logger.error(f"Failed to execute validation query in BigQuery: {e}")
        raise e

    # Parse query records and record validation errors
    deviation_count = 0
    logs_output = []
    
    for row in results:
        status_msg = row.status_msg
        logs_output.append(status_msg)
        if status_msg.startswith("ABWEICHUNG"):
            deviation_count += 1
            
    # Echo diagnostic logs to STDOUT
    for line in logs_output:
        print(line)
        
    print(f"Anzahl gefundener Abweichungen: {deviation_count}")
    
    # 2. CHARACTER-FOR-CHARACTER LOG PRESERVATION (Warning Message)
    if deviation_count > 0:
        # Replicates legacy f_alis_msgerr format with exact warning layout
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_file_stub = f"abgl_kunde_woech_{run_id}.log"
        print(f"[W] {timestamp} {deviation_count} Abweichungen im Kundenadressabgleich gefunden, siehe {log_file_stub}")
        
    print("Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet")
    
    # 3. CHARACTER-FOR-CHARACTER LOG PRESERVATION (Automic JS XML Completion Event)
    print(f"Kundenadressabgleich fuer Lauf {lauf_woche} angestossen")