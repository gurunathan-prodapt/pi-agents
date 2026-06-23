# This Python script provides an example orchestrator to invoke the BigQuery Stored Procedure.
# Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

import argparse
from google.cloud import bigquery
from datetime import datetime

def invoke_bigquery_stored_procedure(
    project_id: str,
    dataset_id: str,
    job_kennung: str,
    eintrags_nr: str,
    stichtag: str, # DDMMYYYY
    wiederanlauf_wert: str
):
    """
    Invokes the BigQuery Stored Procedure `r_ausd_bp_ta_cntrct_dist`.
    """
    client = bigquery.Client(project=project_id)
    sp_name = f"{project_id}.{dataset_id}.r_ausd_bp_ta_cntrct_dist"

    # Construct the SQL query to call the stored procedure
    # Parameters need to be quoted as strings in the SQL call
    query = f"""
    CALL {sp_name}(
        '{job_kennung}',
        '{eintrags_nr}',
        '{stichtag}',
        '{wiederanlauf_wert}'
    );
    """

    print(f"Calling BigQuery Stored Procedure: {sp_name}")
    print(f"With parameters: JobKennung='{job_kennung}', EintragsNr='{eintrags_nr}', Stichtag='{stichtag}', WiederanlaufWert='{wiederanlauf_wert}'")
    print("-" * 50)

    try:
        query_job = client.query(query)
        # Wait for the job to complete
        query_job.result()
        print("-" * 50)
        print(f"Stored Procedure {sp_name} executed successfully.")

        # You can fetch results if the stored procedure returns any
        # For example, if it returns a SELECT statement, you can iterate:
        # for row in query_job:
        #     print(row)

    except Exception as e:
        print(f"Error calling Stored Procedure {sp_name}: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Invoke BigQuery Stored Procedure r_ausd_bp_ta_cntrct_dist."
    )
    parser.add_argument(
        "--project_id",
        required=True,
        help="Google Cloud Project ID."
    )
    parser.add_argument(
        "--dataset_id",
        required=True,
        help="BigQuery Dataset ID where the stored procedure is located."
    )
    parser.add_argument(
        "--job_kennung",
        required=True,
        help="Job identifier (p_JobKennung)."
    )
    parser.add_argument(
        "--eintrags_nr",
        required=True,
        help="Entry number (p_EintragsNr)."
    )
    parser.add_argument(
        "--stichtag",
        required=False,
        default=datetime.today().strftime('%d%m%Y'), # Default to today's date in DDMMYYYY
        help="Reference date for the job in DDMMYYYY format (p_Stichtag). Defaults to today."
    )
    parser.add_argument(
        "--wiederanlauf_wert",
        required=False,
        default="0",
        help="Restart value (p_wiederanlaufWert). Defaults to '0'."
    )

    args = parser.parse_args()

    # Example usage:
    # python invoke_r_ausd_bp_ta_cntrct_dist.py \
    #   --project_id your-gcp-project \
    #   --dataset_id your_bigquery_dataset \
    #   --job_kennung BERT_AUSL_TA_CNTRCT_DIST \
    #   --eintrags_nr 12345 \
    #   --stichtag 01012023 \
    #   --wiederanlauf_wert 0

    invoke_bigquery_stored_procedure(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        job_kennung=args.job_kennung,
        eintrags_nr=args.eintrags_nr,
        stichtag=args.stichtag,
        wiederanlauf_wert=args.wiederanlauf_wert
    )