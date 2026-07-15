from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

DEFAULT_ARGS = {
    'owner': 'migration_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_composer_variables(**kwargs):
    gcs_bucket = Variable.get("GCS_BUCKET", default_var="gcp-dwh-environment-bucket")
    home_uri = f"gs://{gcs_bucket}/dw_source"
    
    variables_map = {
        "DW_DIR_ROOT": f"{home_uri}/aktuell",
        "DW_DIR_PROT": f"{home_uri}/daten/logfiles",
        "DW_DIR_CUBES": f"{home_uri}/daten/cubes",
        "DW_DIR_IMP_D1": f"{home_uri}/daten/d1",
        "DW_DIR_IMP_BWA": f"{home_uri}/daten/dpps/bwa",
        "DW_DIR_IMP_XTRA": f"{home_uri}/daten/xtra",
        "DW_DIR_IMP_CTEL": f"{home_uri}/daten/ctel",
        "DW_DIR_IMP_VO": f"{home_uri}/daten/vo",
        "DW_DIR_IMP_RV": f"{home_uri}/daten/rv",
        "DW_DIR_IMP_IF": f"{home_uri}/daten/ees",
        "DW_DIR_IMP_NNV": f"{home_uri}/daten/nnv",
        "DW_DIR_IMP_SIGMA": f"{home_uri}/daten/gd/sigma",
        "DW_DIR_EXP_SIGMA": f"{home_uri}/daten/gd/sigma/export",
        "DW_DIR_IMP_TRF": f"{home_uri}/daten/trf",
        "DW_DIR_IMP_AUF": f"{home_uri}/daten/sd/auf",
        "DW_DIR_IMP_GUT": f"{home_uri}/daten/sd/gut",
        "DW_DIR_IMP_KDG": f"{home_uri}/daten/sd/kdg",
        "DW_DIR_IMP_MP_KDG": f"{home_uri}/daten/mp/kdg",
        "DW_DIR_IMP_MP_TS": f"{home_uri}/daten/mp/ts",
        "DW_DIR_IMP_MP_ZM": f"{home_uri}/daten/mp/zm",
        "DW_DIR_IMP_TS": f"{home_uri}/daten/sd/ts",
        "DW_DIR_IMP_ZM": f"{home_uri}/daten/sd/zm",
        "DW_DIR_EXP": f"{home_uri}/daten/exporter",
        "DW_DIR_IMP_BPM": f"{home_uri}/daten/bm",
        "DW_DIR_IMP_ZTS": f"{home_uri}/daten/zts",
        "DW_DIR_IMP_VRS": f"{home_uri}/daten/vrs",
        "DW_DIR_IMP_BRUNET": f"{home_uri}/daten/brunet",
        "DW_DIR_IMP_DWH": f"{home_uri}/daten/dwh",
        "DW_DIR_IMP_PLATO": f"{home_uri}/daten/dwh/plato",
        "DW_DIR_IMP_CARMEN": f"{home_uri}/daten/carmen",
        "DW_DIR_IMP_SAP": f"{home_uri}/daten/sap",
        "DW_DIR_IMP_SR_RV": f"{home_uri}/daten/sap/sr_rv_dpps",
        "DW_DIR_IMP_SAP_L": f"{home_uri}/daten/sap/sap_l_gutgr",
        "DW_DIR_IMP_L_MAHNSTYP_IST": f"{home_uri}/daten/sap/mahn",
        "DW_DIR_IMP_L_MAHNV_FI": f"{home_uri}/daten/sap/mahn",
        "DW_DIR_IMP_L_MAHNV_IST": f"{home_uri}/daten/sap/mahn",
        "DW_DIR_IMP_L_GUTGR": f"{home_uri}/daten/sd/l_gutschr",
        "DW_DIR_IMP_L_LEIST": f"{home_uri}/daten/sd/l_leist",
        "DW_DIR_IMP_L_PROD": f"{home_uri}/daten/sd/l_prod",
        "DW_DIR_IMP_LKODE": f"{home_uri}/daten/sd/lkode",
        "DW_DIR_IMP_SUBSE": f"{home_uri}/daten/subse",
        "DW_DIR_SMS_PRG": f"{home_uri}/aktuell/allgemein/is/util",
        "DW_DIR_SMS_ADR": f"{home_uri}/daten/sms/adressen",
        "DW_DIR_SMS_TMP": f"{home_uri}/daten/sms/tmp",
        "DW_DIR_IMP_DPPS": f"{home_uri}/daten/dpps",
        "DW_DIR_IMP_PLANF2": f"{home_uri}/daten/planf2",
        "DW_DIR_UTL_FILE": "/appl/local/oracle/admin/eDWH3/utl_file"
    }
    
    for name, value in variables_map.items():
        Variable.set(name, value)

with DAG(
    'dw_shared_files_bootstrap',
    default_args=DEFAULT_ARGS,
    description='Bootstrap Legacy Variables into Cloud Composer and BigQuery',
    schedule_interval='@once',
    catchup=False,
) as dag:

    bootstrap_env_vars = PythonOperator(
        task_id='bootstrap_env_vars',
        python_callable=load_composer_variables,
    )

    verify_bq_metadata_schema = BigQueryInsertJobOperator(
        task_id='verify_bq_metadata_schema',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TEMP TABLE temp_session_test AS
                    SELECT 
                      '{{ var.value.DW_DIR_ROOT }}' AS bq_dw_dir_root,
                      '{{ var.value.DW_DIR_PROT }}' AS bq_dw_dir_prot,
                      CURRENT_TIMESTAMP() AS validated_at;
                      
                    SELECT * FROM temp_session_test;
                """,
                "useLegacySql": False,
            }
        },
    )

    bootstrap_env_vars >> verify_bq_metadata_schema