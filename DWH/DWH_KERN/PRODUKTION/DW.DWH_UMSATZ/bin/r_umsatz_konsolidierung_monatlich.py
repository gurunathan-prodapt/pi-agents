#!/usr/bin/env python3
#-----------------------------------------------------------------------------
#- Ersterstellung am:                           2015-11-03T09:00:00+01:00
#- Ersterstellung durch:                        rvogel
#- Aenderung:                                   2019-06-11  mschaefer  - Umstellung auf Konzerngesellschafts-Parameter
#- Aenderung:                                   2022-01-20  khoffmann  - Toleranzpruefung ergaenzt
#-----------------------------------------------------------------------------
"""
Description: Monthly consolidation of sales data (UMSATZ) across all group companies.
             Migrated from r_umsatz_konsolidierung_monatlich.ksh to Horizon Python / BigQuery.
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

PROG_NAME = "Ausfuehrung Script r_umsatz_konsolidierung_monatlich.py"
PROG_VERSION = "1.4.0"

# Set up logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger()

def get_default_month():
    """Returns the previous month in YYYYMM format."""
    previous_month = datetime.now() - relativedelta(months=1)
    return previous_month.strftime('%Y%m')

def parse_arguments():
    """Parses command-line options."""
    parser = argparse.ArgumentParser(
        description=f"{PROG_NAME} (v{PROG_VERSION}): Monthly Sales Consolidation (UMSATZ)."
    )
    parser.add_argument(
        '-m', '--monat',
        type=str,
        default=None,
        help="Processing month in format 'YYYYMM'. Defaults to last month."
    )
    parser.add_argument(
        '-k', '--konzern',
        type=str,
        default='ALL',
        help="Group company identifier (e.g., 'DE01', 'AT02', 'ALL')."
    )
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    l_Monat = args.monat if args.monat else get_default_month()
    l_Konzern = args.konzern
    
    # EXACT German literal log statements preserved from source ksh wrapper
    print(f"Starte monatliche Umsatzkonsolidierung fuer Monat {l_Monat}, Konzerngesellschaft {l_Konzern}")
    
    try:
        # Establish Cloud Environment Configs
        gcp_project = os.environ.get("GCP_PROJECT")
        bq_dataset = os.environ.get("BQ_DATASET", "dwh_umsatz_dataset")
        sql_dir = os.environ.get("SQL_DIR", "/home/airflow/gcs/dags/dwh/dwh_kern/produktion/dw_dwh_umsatz/sql")
        
        sql_file_path = os.path.join(sql_dir, "umsatz_konsolidierung.sql")
        with open(sql_file_path, "r") as f:
            query_template = f.read()
            
        client = bigquery.Client(project=gcp_project)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("verarbeitungsmonat", "STRING", l_Monat),
                bigquery.ScalarQueryParameter("konzerngesellschaft", "STRING", l_Konzern)
            ]
        ) 
        
        # Format the parameters into the query string
        formatted_query = query_template.format(
            gcp_project=gcp_project,
            bq_dataset=bq_dataset
        )
        
        # Execute Query job
        query_job = client.query(formatted_query, job_config=job_config)
        result = query_job.result()  # Waits for job to complete
        
        # Check execution status for simulating the original process returns
        if query_job.errors:
            l_RetCode = 1
            print(f"[E] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Umsatzkonsolidierung fuer Monat {l_Monat}/{l_Konzern} mit Fehlercode {l_RetCode} abgebrochen", file=sys.stderr)
            sys.exit(l_RetCode)
            
    except Exception as err:
        l_RetCode = 1
        print(f"[E] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Umsatzkonsolidierung fuer Monat {l_Monat}/{l_Konzern} mit Fehlercode {l_RetCode} abgebrochen", file=sys.stderr)
        print(f"[E] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 1 Fehlerzeilen im Konsolidierungs-Protokoll gefunden, siehe Exception detail: {err}", file=sys.stderr)
        sys.exit(l_RetCode)

    # EXACT German success log statement preserved
    print("Monatliche Umsatzkonsolidierung ohne erkennbare Fehler beendet")
    sys.exit(0)

if __name__ == "__main__":
    main()