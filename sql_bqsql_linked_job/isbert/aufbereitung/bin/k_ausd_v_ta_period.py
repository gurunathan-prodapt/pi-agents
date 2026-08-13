#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Control and wrapper script that validates parameters and executes the SQL script
d_ausd_v_ta_period.sql synchronously via BigQuery Python client.
"""

import os
import sys
from google.cloud import bigquery

def log_error(err_nr, err_arg):
    # Simulated equivalent of DWMSG_MeldeFehler / echo FEHLER
    print(f"FEHLER: 0 E {err_nr} {err_arg}")
    print("Bitte ueber Rahmenscript aufrufen")
    sys.exit(err_nr)

def main():
    p_JobKennung = None
    p_EintragsNr = None
    ErrNr = 0
    ErrArg = ""

    # Parse arguments manually to replicate getopts behaviour exactly
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == '-h':
            print("Bitte ueber Rahmenscript aufrufen")
            sys.exit(0)
        elif arg == '-j':
            if i + 1 < len(args):
                p_JobKennung = args[i+1]
                i += 2
            else:
                ErrNr = 193
                ErrArg = "j"
                i += 1
        elif arg == '-f':
            if i + 1 < len(args):
                p_EintragsNr = args[i+1]
                i += 2
            else:
                ErrNr = 193
                ErrArg = "f"
                i += 1
        elif arg.startswith('-'):
            ErrNr = 192
            ErrArg = arg[1:]
            i += 1
        else:
            # Positional arguments or unrecognized
            ErrNr = 192
            ErrArg = arg
            i += 1

    # setze Tabellenname
    v_TabName = 'ta_period'

    # Pruefe, ob notwendige Parameter gesetzt worden sind
    if ErrNr == 0:
        if not p_JobKennung:
            ErrNr = 193
            ErrArg = "Jobkennung"
        elif not p_EintragsNr:
            ErrNr = 193
            ErrArg = "EintragsNr"

    if ErrNr != 0:
        log_error(ErrNr, ErrArg)

    # Environment Path Verification
    bert_dir_root = os.environ.get("BERT_DIR_ROOT", "/app")
    gcp_project = os.environ.get("GCP_PROJECT")
    bq_dataset = os.environ.get("BQ_DATASET")

    # SQL-Skript Path
    Name_SQLskript = os.path.join(bert_dir_root, "aufbereitung", "sql", "d_ausd_v_ta_period.sql")

    # Read SQL file
    try:
        with open(Name_SQLskript, "r", encoding="utf-8") as f:
            sql_content = f.read()
    except Exception as e:
        print(f"Error reading SQL file {Name_SQLskript}: {e}", file=sys.stderr)
        sys.exit(1)

    # Perform dynamic replacements of environment variables in the SQL statement
    if gcp_project:
        sql_content = sql_content.replace("${GCP_PROJECT}", gcp_project)
        sql_content = sql_content.replace("$GCP_PROJECT", gcp_project)
    if bq_dataset:
        sql_content = sql_content.replace("${BQ_DATASET}", bq_dataset)
        sql_content = sql_content.replace("$BQ_DATASET", bq_dataset)

    # DB-Script ausfuehren synchronously using BigQuery Client
    try:
        client = bigquery.Client(project=gcp_project)
        
        # Prepare query parameters
        query_parameters = [
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", p_EintragsNr),
            bigquery.ScalarQueryParameter("job_kennung", "STRING", p_JobKennung)
        ]
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters
        )
        
        query_job = client.query(sql_content, job_config=job_config)
        # Wait for query to complete
        results = query_job.result()
        
        # Capture metrics
        v_records = query_job.num_dml_affected_rows
        if v_records is None:
            if query_job.total_rows is not None:
                v_records = query_job.total_rows
            else:
                v_records = 0
                
    except Exception as err:
        print(f"ERROR: Database query execution failed: {err}", file=sys.stderr)
        sys.exit(1)

    print(" ---------- ENDE Datenverarbeitung ----------")

    # Log metrics
    print(f"Records processed: {v_records}")

if __name__ == "__main__":
    main()