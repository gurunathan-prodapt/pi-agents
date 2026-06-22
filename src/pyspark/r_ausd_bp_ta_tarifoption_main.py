# Legacy Source: r_ausd_bp_ta_tarifoption.ksh, k_ausd_bp_ta_tarifoption.ksh
# Job: DW.BERT_AUSD_BP_TA_TARIFOPTION

import argparse
import logging
from datetime import datetime, timedelta

from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_bq_client():
    """Returns a BigQuery client."""
    return bigquery.Client()

def get_v_datum(client: bigquery.Client) -> str:
    """
    Determines the v_datum similar to the Oracle script's logic.
    SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    """
    query = """
    SELECT
        IFNULL(FORMAT_DATE('%%Y%%m%%d', DATE(MAX(m.timecreated))), '19000101') AS s_datum
    FROM
        `isbert_schema.dwtk_meldungen` m
    WHERE
        m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    try:
        query_job = client.query(query)
        result = query_job.result()
        for row in result:
            return row.s_datum
    except Exception as e:
        logging.error(f"Error determining v_datum: {e}")
        raise
    return '19000101' # Fallback, though an exception is better for critical path.


def execute_bigquery_sql_template(sql_template_path: str, params: dict):
    """
    Reads a BigQuery SQL template, formats it with parameters, and executes it.
    """
    logging.info(f"Executing BigQuery SQL from: {sql_template_path}")
    try:
        with open(sql_template_path, 'r') as f:
            sql_template = f.read()

        # BigQuery client
        client = get_bq_client()

        # Determine v_datum here as the SQL script expects it to be declared internally or calculated.
        # For simplicity, if v_datum is already in the template, we rely on the template's DECLARE/SET.
        # If PySpark needs to provide a variable for dynamic table names, we'd format that into the SQL.
        # The provided SQL template has DECLARE v_datum; SET v_datum = ... which is self-contained.
        # However, for the EXECUTE IMMEDIATE FORMAT part which depends on v_datum, we need to ensure
        # the SQL script calculates it *before* attempting the EXECUTE IMMEDIATE.

        # The current SQL template itself calculates v_datum using DECLARE/SET.
        # So we just execute the whole template.

        # For the dynamic table name in the intermediate table creation, the SQL template
        # uses EXECUTE IMMEDIATE FORMAT. This means the SQL script is self-sufficient
        # in determining `v_datum` and using it.
        # If the v_datum needed to be passed from PySpark into the SQL,
        # we would do:
        # formatted_sql = sql_template.format(v_datum=params.get('v_datum', get_v_datum(client)))
        # For now, we'll execute the script as is.

        # For multi-statement BigQuery SQL, it's often executed as a script.
        # The BigQuery client can run multiple statements if they are separated by semicolons.
        # However, EXECUTE IMMEDIATE is a single statement.
        # If the SQL file contains multiple top-level statements or scripting,
        # ensure it's compatible with how `client.query` handles it.
        # For multi-statement transactions, jobs might need to be chained.
        # For now, assuming the SQL file is treated as a single script execution context.
        query_job = client.query(sql_template)
        query_job.result() # Wait for job to complete
        logging.info(f"BigQuery SQL execution completed successfully.")

    except Exception as e:
        logging.error(f"Error executing BigQuery SQL: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="PySpark script for DW.BERT_AUSD_BP_TA_TARIFOPTION migration.")
    parser.add_argument('--stichtag', type=str, help='Stichtag (reference date) for processing (YYYY-MM-DD)', default=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
    parser.add_argument('--wiederanlaufwert', type=str, help='Wiederanlaufwert (restart value)', default='initial')
    parser.add_argument('--sql_template_path', type=str, required=True, help='Path to the BigQuery SQL template file.')

    args = parser.parse_args()

    logging.info(f"Starting PySpark job with Stichtag: {args.stichtag}, Wiederanlaufwert: {args.wiederanlaufwert}")

    # Parameters for the SQL script (if it were to consume them directly)
    sql_params = {
        'stichtag': args.stichtag,
        'wiederanlaufwert': args.wiederanlaufwert,
        # v_datum is calculated within the SQL script for EXECUTE IMMEDIATE,
        # but if needed in PySpark for external logic, it can be derived here.
        # v_datum: get_v_datum(get_bq_client()) # if needed in python
    }

    try:
        execute_bigquery_sql_template(args.sql_template_path, sql_params)
        logging.info("PySpark job completed successfully.")
    except Exception as e:
        logging.error(f"PySpark job failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()