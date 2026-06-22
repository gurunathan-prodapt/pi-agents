#
# Airflow DAG for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh
#
# Replaces legacy KornShell script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh
#

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.models.variable import Variable
from datetime import datetime

def _parse_and_validate_params(**kwargs):
    """
    Parses and validates DAG run parameters for stichtag and wiederanlaufwert.
    Replaces original shell script's getopts and parameter validation logic.
    """
    ti = kwargs['ti']
    dag_run = kwargs.get('dag_run')

    # Default system date in DDMMYYYY format, used if stichtag is not explicitly provided
    v_sysdate_ddmmyyyy = datetime.now().strftime('%d%m%Y')

    # Retrieve stichtag from DAG run configuration.
    # Prefer YYYY-MM-DD as input, but accept DDMMYYYY.
    # If not provided, default to current system date (DDMMYYYY).
    raw_stichtag_param = dag_run.conf.get('stichtag') if dag_run and dag_run.conf else None
    
    p_stichtag_ddmmyyyy = None
    if raw_stichtag_param:
        try:
            # Try parsing as YYYY-MM-DD (Airflow's common format)
            dt_stichtag = datetime.strptime(raw_stichtag_param, '%Y-%m-%d')
            p_stichtag_ddmmyyyy = dt_stichtag.strftime('%d%m%Y')
        except ValueError:
            try:
                # If not YYYY-MM-DD, try DDMMYYYY (original script format)
                dt_stichtag = datetime.strptime(raw_stichtag_param, '%d%m%Y')
                p_stichtag_ddmmyyyy = raw_stichtag_param
            except ValueError:
                raise ValueError(f"Invalid stichtag format: {raw_stichtag_param}. Expected YYYY-MM-DD or DDMMYYYY.")
    else:
        # If no stichtag provided in conf, use the determined v_sysdate_ddmmyyyy
        p_stichtag_ddmmyyyy = v_sysdate_ddmmyyyy

    # Retrieve wiederanlaufwert from DAG run configuration, default to 0
    wiederanlaufwert = dag_run.conf.get('wiederanlaufwert', 0) if dag_run and dag_run.conf else 0
    
    # Determine JobKennung and DW_EintragsNr as in the original script
    # JobKennung: fixed string
    job_kennung = "ausd_bp_ta_bpr_bcp"
    # DW_EintragsNr: use Airflow's unique run_id for tracking
    dw_eintragsnr = kwargs['run_id'] 

    print(f"Parameters for core processing:")
    print(f"  Stichtag (DDMMYYYY): {p_stichtag_ddmmyyyy}")
    print(f"  Wiederanlaufwert: {wiederanlaufwert}")
    print(f"  JobKennung: {job_kennung}")
    print(f"  DW_EintragsNr: {dw_eintragsnr}")

    # Push parameters to XCom for use by downstream tasks
    ti.xcom_push(key='p_stichtag_ddmmyyyy', value=p_stichtag_ddmmyyyy)
    ti.xcom_push(key='p_wiederanlaufwert', value=wiederanlaufwert)
    ti.xcom_push(key='job_kennung', value=job_kennung)
    ti.xcom_push(key='dw_eintragsnr', value=dw_eintragsnr)


# DAG definition
with DAG(
    dag_id="r_ausd_bp_ta_bpr_bcp_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None, # This DAG is likely triggered manually or externally
    catchup=False,
    tags=["bert", "aufbereitung", "basisprodukte"],
    params={
        "stichtag": {
            "type": "string",
            "title": "Stichtag (Key Date)",
            "description": "Key date for data extraction (format YYYY-MM-DD or DDMMYYYY). Defaults to current system date.",
            "default": datetime.now().strftime('%Y-%m-%d'), # Default for UI/manual trigger
        },
        "wiederanlaufwert": {
            "type": "integer",
            "title": "Wiederanlaufwert (Restart Value)",
            "description": "If set, only contracts with DWH_VERTRAG_ID > this value are processed. Defaults to 0.",
            "default": 0,
        },
    },
    doc_md="""
    ### `r_ausd_bp_ta_bpr_bcp_dag`

    This Airflow DAG replaces the legacy KornShell script `r_ausd_bp_ta_bpr_bcp.ksh`.

    **Purpose:**
    Initial provisioning of selected "Basisprodukte" (base products) for the BERT system.
    This job generates a key-date extract of the contract cache in the DWH and makes it available
    for Forderungsscoring.

    **Original Script Description (`usage` function):**
    Programm: Bereitstellung Basisprodukte BERT
    Version: V2.0.0
    Aufruf:   `airflow dags trigger r_ausd_bp_ta_bpr_bcp_dag --conf '{"stichtag": "YYYY-MM-DD", "wiederanlaufwert": 0}'`
    Parameters:
    - `stichtag`: Key date in YYYY-MM-DD or DDMMYYYY format. If not set, defaults to current system date.
    - `wiederanlaufwert`: If set, only contracts with DWH_VERTRAG_ID > this value are processed.
                        Entries regarding values >= this value will be deleted. Defaults to 0.

    **Description:**
    This job creates a key-date extract of the contract cache in the DWH and provides it to
    Forderungsscoring. Note that an already provisioned table will be deleted if no active
    contract cache exists that has not yet been fetched. Such a fetch must have been marked
    accordingly by the FOS-Loader. Records are selected for which
    `Gueltig_von <= Stichtag < Gueltig_bis` AND `LADEDATUM < Stichtag` applies.
    If the key date is not set, the MINIMUM of the current system date and the maximum
    loading date (source table) is used.
    """
) as dag:
    # Task to parse and validate parameters
    parse_validate_params = PythonOperator(
        task_id="parse_validate_params",
        python_callable=_parse_and_validate_params,
        provide_context=True, # Allows access to dag_run, ti, ds, etc.
    )

    # Task to run the core processing logic, replacing k_ausd_bp_ta_bpr_bcp.ksh
    # This is a placeholder and assumes k_ausd_bp_ta_bpr_bcp.ksh is migrated to BigQuery SQL.
    # The actual SQL query would come from the migration of k_ausd_bp_ta_bpr_bcp.ksh.
    # For now, it's a dummy SELECT statement.
    run_core_processing = BigQueryOperator(
        task_id="run_core_processing",
        sql='''
        -- Placeholder for the actual BigQuery SQL logic of k_ausd_bp_ta_bpr_bcp.ksh
        -- This SQL should perform the data extraction and staging operations.
        -- Parameters passed from the upstream task via XCom:
        -- job_kennung: {{ ti.xcom_pull(task_ids="parse_validate_params", key="job_kennung") }}
        -- stichtag (DDMMYYYY): {{ ti.xcom_pull(task_ids="parse_validate_params", key="p_stichtag_ddmmyyyy") }}
        -- wiederanlaufwert: {{ ti.xcom_pull(task_ids="parse_validate_params", key="p_wiederanlaufwert") }}
        -- dw_eintragsnr: {{ ti.xcom_pull(task_ids="parse_validate_params", key="dw_eintragsnr") }}

        -- Example BigQuery SQL (replace with actual logic)
        SELECT
            '{{ ti.xcom_pull(task_ids="parse_validate_params", key="job_kennung") }}' AS job_kennung,
            '{{ ti.xcom_pull(task_ids="parse_validate_params", key="p_stichtag_ddmmyyyy") }}' AS stichtag,
            CAST('{{ ti.xcom_pull(task_ids="parse_validate_params", key="p_wiederanlaufwert") }}' AS INT64) AS wiederanlaufwert,
            '{{ ti.xcom_pull(task_ids="parse_validate_params", key="dw_eintragsnr") }}' AS dw_eintragsnr,
            CURRENT_TIMESTAMP() AS processing_timestamp,
            'Processed k_ausd_bp_ta_bpr_bcp.ksh logic (placeholder)' AS status;
        -- You might want to insert results into a specific table or perform DML operations here.
        ''',
        use_legacy_sql=False, # Use standard SQL
        gcp_conn_id='google_cloud_default', # Assumes a Google Cloud connection is configured in Airflow
        # destination_dataset_table='your_project.your_dataset.your_target_table', # Uncomment and configure if results need to be stored
        # write_disposition='WRITE_TRUNCATE', # Or 'WRITE_APPEND', 'WRITE_EMPTY'
        # create_disposition='CREATE_IF_NEEDED', # Or 'CREATE_NEVER'
        params={
            "bert_dir_root": Variable.get("BERT_DIR_ROOT_AIRFLOW_VAR_NAME", default="/usr/local/airflow/bert_root")
        } # Example of passing Airflow Variable to BigQueryOperator
    )

    # Define task dependencies
    parse_validate_params >> run_core_processing