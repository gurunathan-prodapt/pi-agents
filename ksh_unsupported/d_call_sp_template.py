#!/usr/bin/env python3
#                               -*- Mode: Sh -*-
# d_call_sp_template.ksh --- Beispiel fuer einen SP Aufruf von Shell aus
# Autor               : Thomas Bregulla
# Erzeugt am          : Fri Feb  6 23:08:32 1998
# Letzte Aenderung von: Thomas Bregulla
# Letzte Aenderung am : Fri Feb  6 23:11:33 1998
# Status              : Unbekannt, bitte Vorsicht!
# $Id$
# $Locker$
# Versions-Anmerkungen
# $Log$
# 
# Zweck
#     Beispielhafter Aufruf einer Stored Procedure

import os
import sys
import argparse
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

def call_bigquery_stored_procedure(fachl_name1: str, fachl_name2: str):
    """
    Executes the BigQuery equivalent of the legacy Oracle Stored Procedure.
    Uses Google Cloud IAM authentication derived from the runtime environment.
    """
    # Environment variable resolutions (No prose placeholders)
    gcp_project = os.environ.get("GCP_PROJECT")
    bq_dataset = os.environ.get("BQ_DATASET")
    bq_location = os.environ.get("BQ_LOCATION", "EU")

    if not gcp_project or not bq_dataset:
        print("Error: Required environment variables GCP_PROJECT or BQ_DATASET are missing.", file=sys.stderr)
        sys.exit(1)

    # Initialize native BigQuery Client
    client = bigquery.Client(project=gcp_project, location=bq_location)

    # The migrated stored procedure identifier (fully-qualified)
    # Mapping legacy 'd_call_sp_template.sql' script invocation to BQ procedure call
    procedure_name = f"`{gcp_project}.{bq_dataset}.d_call_sp_template`"
    
    query = f"CALL {procedure_name}(@param1, @param2);"
    
    # Configure parameters to prevent SQL injection and map parameters safely
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("param1", "STRING", fachl_name1),
            bigquery.ScalarQueryParameter("param2", "STRING", fachl_name2),
        ]
    )

    try:
        print(f"Starting execution of stored procedure {procedure_name}...")
        query_job = client.query(query, job_config=job_config)
        
        # Wait for stored procedure execution to complete
        query_job.result()
        print("Stored procedure execution completed successfully in BigQuery.")

    except GoogleCloudError as e:
        print(f"Error during BigQuery stored procedure execution: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected application error occurred: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Migrated Python launcher for BigQuery Stored Procedure d_call_sp_template"
    )
    parser.add_argument("fachl_name1", help="First functional parameter (fachl_name1)")
    parser.add_argument("fachl_name2", help="Second functional parameter (fachl_name2)")
    args = parser.parse_args()

    call_bigquery_stored_procedure(args.fachl_name1, args.fachl_name2)

if __name__ == "__main__":
    main()