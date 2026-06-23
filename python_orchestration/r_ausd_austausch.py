#
# Python orchestration script for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
# This script demonstrates how to call the BigQuery Stored Procedure.
# It can be adapted for Cloud Functions, Cloud Run, or Cloud Composer (Airflow).
#

import argparse
import datetime
from google.cloud import bigquery

def run_bert_ausd_austausch(stichtag_string: str = None, wiederanlauf_wert: int = None):
    """
    Executes the BigQuery Stored Procedure for r_ausd_austausch.

    Args:
        stichtag_string: Optional snapshot date in 'DDMMYYYY' format.
                         If None, defaults to current date in BigQuery SP.
        wiederanlauf_wert: Optional restart value.
                           If None, defaults to 0 in BigQuery SP.
    """
    client = bigquery.Client()
    project_id = client.project # Automatically gets the default project ID
    dataset_id = 'bert_reporting'
    procedure_id = 'r_ausd_austausch_sp'

    # Construct the SQL for calling the stored procedure
    # Parameters need to be properly formatted for BigQuery SQL
    stichtag_param = f"'{stichtag_string}'" if stichtag_string else 'NULL'
    wiederanlauf_param = str(wiederanlauf_wert) if wiederanlauf_wert is not None else 'NULL'

    query = f"CALL `{project_id}.{dataset_id}.{procedure_id}`({stichtag_param}, {wiederanlauf_param});"

    print(f"Executing BigQuery Stored Procedure: {query}")

    try:
        query_job = client.query(query)
        query_job.result()  # Waits for the job to complete
        print(f"Stored Procedure `{dataset_id}.{procedure_id}` executed successfully.")
        return 0
    except Exception as e:
        print(f"Error executing Stored Procedure: {e}")
        # In a real-world scenario, you might log this to Cloud Logging
        # and potentially send alerts.
        return 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Run BigQuery Stored Procedure for BERT r_ausd_austausch."
    )
    parser.add_argument(
        "-s",
        "--stichtag",
        help="Snapshot date in DDMMYYYY format (e.g., 28022023). Defaults to current date if not provided.",
        type=str,
        default=None,
    )
    parser.add_argument(
        "-l",
        "--wiederanlauf",
        help="Restart value (integer). Defaults to 0 if not provided.",
        type=int,
        default=None,
    )

    args = parser.parse_args()

    exit_code = run_bert_ausd_austausch(args.stichtag, args.wiederanlauf)
    exit(exit_code)