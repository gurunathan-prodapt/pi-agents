# Python wrapper script for DW.BERT_AUSD_BP_TA_APN_VERTRAG
# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
# This script handles parameter parsing, date validation, and prepares BigQuery SQL for execution.

import os
import argparse
import datetime
from airflow.models import Variable # For potential future use with Airflow Variables

def main(job_kennung: str, stichtag: str, eintragsnr: int, wiederanlaufwert: int, **context):
    """
    Main function for the k_ausd_bp_ta_apn_vertrag_wrapper.
    Replaces the KornShell script logic, including parameter parsing,
    date validation, and preparing BigQuery SQL.

    Args:
        job_kennung (str): Job identifier (e.g., 'AUSD_BP_TA_APN_VERTRAG').
        stichtag (str): Processing date in YYYYMMDD format.
        eintragsnr (int): Entry number.
        wiederanlaufwert (int): Restart value.
        **context: Airflow context for XComs.
    """
    print(f"Starting k_ausd_bp_ta_apn_vertrag_wrapper with parameters:")
    print(f"  JobKennung: {job_kennung}")
    print(f"  Stichtag: {stichtag}")
    print(f"  EintragsNr: {eintragsnr}")
    print(f"  Wiederanlaufwert: {wiederanlaufwert}")

    # --- Parameter Validation ---
    # Replaces KornShell's pruefeParameterGesetzt and DWDate_Datum_Check
    if not stichtag:
        raise ValueError("Stichtag (date) parameter is missing.")
    try:
        # Validate stichtag format
        datetime.datetime.strptime(stichtag, "%Y%m%d")
        print(f"Stichtag {stichtag} is valid (YYYYMMDD format).")
    except ValueError:
        raise ValueError(f"Invalid Stichtag format: {stichtag}. Expected YYYYMMDD.")

    # --- Load BigQuery SQL Template ---
    # The BigQuery SQL script is expected to be in the same directory as this wrapper.
    # In a production environment, this might be loaded from Google Cloud Storage.
    sql_template_file = "d_ausd_bp_ta_apn_vertrag_bq.sql"
    sql_template_path = os.path.join(os.path.dirname(__file__), sql_template_file)

    try:
        with open(sql_template_path, "r") as f:
            bq_sql_template = f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"BigQuery SQL template not found at: {sql_template_path}")
    
    # The v_datum declaration is handled within the BQSQL itself.
    # If other dynamic values (e.g., from parameters) were needed in the SQL,
    # they would be rendered here using Python's string formatting or a templating engine like Jinja2.
    rendered_bq_sql = bq_sql_template
    
    # --- Push rendered SQL to XComs ---
    # The `context` argument provides access to Airflow's TaskInstance (ti) object.
    task_instance = context["ti"]
    task_instance.xcom_push(key="bq_sql_to_execute", value=rendered_bq_sql)

    print(f"Successfully loaded and pushed BigQuery SQL from {sql_template_file} to XComs.")

# This allows the script to be tested independently outside of Airflow.
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KornShell to Python wrapper for APN Contract processing.")
    parser.add_argument("-j", "--job_kennung", required=True, help="Job identifier (e.g., AUSD_BP_TA_APN_VERTRAG).")
    parser.add_argument("-s", "--stichtag", required=True, help="Processing date in YYYYMMDD format.")
    parser.add_argument("-f", "--eintragsnr", type=int, default=1, help="Entry number.")
    parser.add_argument("-l", "--wiederanlaufwert", type=int, default=0, help="Restart value.")
    
    args = parser.parse_args()

    # Mock Airflow's TaskInstance for local testing
    class MockTaskInstance:
        def xcom_push(self, key, value):
            print(f"Mock XCom Push: key='{key}', value (truncated):\n{value[:500]}...") # Print first 500 chars of SQL
            self.xcom_value = value # Store for inspection in tests

    mock_ti = MockTaskInstance()
    mock_context = {"ti": mock_ti}

    try:
        main(args.job_kennung, args.stichtag, args.eintragsnr, args.wiederanlaufwert, **mock_context)
        print("\nStandalone execution complete. Check mock_ti.xcom_value for pushed SQL.")
    except Exception as e:
        print(f"\nStandalone execution failed: {e}")