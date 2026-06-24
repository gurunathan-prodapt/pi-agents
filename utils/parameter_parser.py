# Migrated utility: h_alis_parameter.ksh (Parameter Parsing)
# Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
# This file provides basic parameter parsing utilities.
# In an Airflow context, DAG parameters (e.g., via `params` or `dag_run.conf`)
# are the primary mechanism for passing configuration, making a dedicated
# Python parameter parser less critical for the DAG itself.
# However, this can be used for parsing environment variables or specific
# command-line arguments if needed in sub-scripts.

import os
import argparse
import logging

logger = logging.getLogger(__name__)

def parse_dag_parameters(dag_run_conf: dict):
    """
    A utility function to extract parameters from Airflow's dag_run.conf.
    This replaces the shell script's getopts logic.
    """
    params = {
        'p_JobKennung': dag_run_conf.get('job_kennung'),
        'p_EintragsNr': dag_run_conf.get('eintrags_nr'),
        'p_Stichtag': dag_run_conf.get('stichtag'),
        'p_wiederanlaufWert': dag_run_conf.get('wiederanlauf_wert', 0) # Default to 0 as in original script
    }
    return params

def pruefeParameterGesetzt(param_name: str, param_value):
    """
    Simulates the pruefeParameterGesetzt function from h_alis_parameter.ksh.
    Checks if a parameter value is set (not None or empty string).
    Raises ValueError if the parameter is not set.
    """
    if param_value is None or (isinstance(param_value, str) and not param_value.strip()):
        raise ValueError(f"Parameter '{param_name}' is not set and is required.")
    logger.info(f"Parameter '{param_name}' is set: '{param_value}'")
    return True

# Example usage (for direct script execution if not using Airflow DAG parameters)
if __name__ == "__main__":
    # Example for parsing command-line arguments (not typically used in Airflow DAGs directly)
    parser = argparse.ArgumentParser(description="Process job parameters.")
    parser.add_argument('-j', '--job_kennung', help='Job identifier')
    parser.add_argument('-f', '--eintrags_nr', help='Entry number')
    parser.add_argument('-s', '--stichtag', help='Reference date (DDMMYYYY)')
    parser.add_argument('-l', '--wiederanlauf_wert', type=int, default=0, help='Restart value')
    args = parser.parse_args()

    print("--- Command Line Parameter Parsing Example ---")
    try:
        pruefeParameterGesetzt("Jobkennung", args.job_kennung)
        pruefeParameterGesetzt("Stichtag", args.stichtag)
        pruefeParameterGesetzt("EintragsNr", args.eintrags_nr)
        print(f"All required command-line parameters are set.")
        print(f"JobKennung: {args.job_kennung}")
        print(f"EintragsNr: {args.eintrags_nr}")
        print(f"Stichtag: {args.stichtag}")
        print(f"WiederanlaufWert: {args.wiederanlauf_wert}")
    except ValueError as e:
        print(f"Error parsing command-line parameters: {e}")

    # Example for parsing Airflow DAG run configuration
    print("\n--- Airflow DAG Parameter Parsing Example ---")
    mock_dag_run_conf = {
        'job_kennung': 'BERT_JOB_ABC',
        'eintrags_nr': '123',
        'stichtag': '25122023'
    }
    dag_params = parse_dag_parameters(mock_dag_run_conf)
    try:
        pruefeParameterGesetzt("Jobkennung", dag_params['p_JobKennung'])
        pruefeParameterGesetzt("Stichtag", dag_params['p_Stichtag'])
        pruefeParameterGesetzt("EintragsNr", dag_params['p_EintragsNr'])
        print(f"All required DAG parameters are set.")
        print(f"p_JobKennung: {dag_params['p_JobKennung']}")
        print(f"p_EintragsNr: {dag_params['p_EintragsNr']}")
        print(f"p_Stichtag: {dag_params['p_Stichtag']}")
        print(f"p_wiederanlaufWert: {dag_params['p_wiederanlaufWert']}")
    except ValueError as e:
        print(f"Error parsing DAG parameters: {e}")