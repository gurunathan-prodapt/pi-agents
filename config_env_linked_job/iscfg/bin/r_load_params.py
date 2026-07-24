import os
import sys
import traceback
from google.cloud import bigquery

def execute_parameter_load():
    rc = 1 # Legacy exit code on failure
    sql_file_path = "config_env_linked_job/iscfg/cfg/d_param_load.sql"
    
    try:
        # Retrieve global variables
        gcp_project = os.environ.get("GCP_PROJECT")
        bq_dataset_adm = os.environ.get("BQ_DATASET_ADM", "DWH_ADM")
        bq_dataset_stg = os.environ.get("BQ_DATASET_STG", "DWH_STG")
        
        if not gcp_project:
            raise ValueError("GCP_PROJECT environment variable is missing.")

        # Read SQL query template
        with open(sql_file_path, "r", encoding="utf-8") as file:
            sql_template = file.read()
        
        # Inject environment variable values
        formatted_query = sql_template.format(
            GCP_PROJECT=gcp_project,
            BQ_DATASET_ADM=bq_dataset_adm,
            BQ_DATASET_STG=bq_dataset_stg
        )
        
        # Initialize BigQuery Client and run query
        client = bigquery.Client()
        query_job = client.query(formatted_query)
        query_job.result() # Blocks until job execution succeeds
        
    except Exception as e:
        # OUTPUT/PRINT LITERAL RULE: Verbatim error message from source preserved exactly
        print(f"FEHLER: d_param_load.sql beendet mit RC={rc}")
        print(f"Detail error: {str(e)}")
        print(traceback.format_exc())
        sys.exit(rc)

if __name__ == "__main__":
    execute_parameter_load()