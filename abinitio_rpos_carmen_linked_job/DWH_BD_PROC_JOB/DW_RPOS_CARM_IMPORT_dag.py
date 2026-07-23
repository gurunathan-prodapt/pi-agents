from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# Sourcing variables from the target platform's native mechanism (Airflow Variables)
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET")

# Job-specific variables
DB_TNS_NAME_DWH = Variable.get("DB_TNS_NAME_DWH", default_var="DWH")
DB_USER_DWH = Variable.get("DB_USER_DWH", default_var="DWH_USER")
DB_PASSWD_DWH = Variable.get("DB_PASSWD_DWH", default_var="DWH_PASS")
BHB_Quellverzeichnis = Variable.get("BHB_Quellverzeichnis", default_var="crs/work/")
BHB_Dateiname = Variable.get("BHB_Dateiname", default_var="CARMEN_B_pos.fix")
BHB_Eintragsnr = Variable.get("BHB_Eintragsnr", default_var="12345")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="DW_RPOS_CARM_IMPORT_dag",
    default_args=default_args,
    schedule_interval=None,  # Event-triggered as per upstream requirements
    catchup=False,
    tags=["DWH", "RPOS", "CARMEN"],
) as dag:

    # 1. Environment and parameter validation using the migrated wrapper script
    validate_env = BashOperator(
        task_id="validate_environment_parameters",
        bash_command=f"python3 gs://{GCS_BUCKET}/scripts/map_rpos_carmen_import.py",
        env={
            "DB_TNS_NAME_DWH": DB_TNS_NAME_DWH,
            "DB_USER_DWH": DB_USER_DWH,
            "DB_PASSWD_DWH": DB_PASSWD_DWH,
            "BHB_Quellverzeichnis": BHB_Quellverzeichnis,
            "BHB_Dateiname": BHB_Dateiname,
            "BHB_Eintragsnr": BHB_Eintragsnr,
            "GCP_PROJECT": GCP_PROJECT,
            "GCS_BUCKET": GCS_BUCKET,
            "BQ_DATASET": BQ_DATASET
        }
    )

    # 2. Spark Job Ingestion submitted to Cloud Dataproc
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/scripts/map_rpos_carmen_import_pyspark.py",
            "environment_variables": {
                "GCP_PROJECT": GCP_PROJECT,
                "GCS_BUCKET": GCS_BUCKET,
                "BQ_DATASET": BQ_DATASET,
                "BHB_Quellverzeichnis": BHB_Quellverzeichnis,
                "BHB_Dateiname": BHB_Dateiname,
                "BHB_Eintragsnr": BHB_Eintragsnr,
                "BHB_Nutzdatensatzkennung": "P",
                "BHB_Endedatensatzkennung": "X"
            }
        }
    }

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_map_rpos_carmen_import_pyspark",
        job=pyspark_job_config,
        region=GCP_REGION,
        project_id=GCP_PROJECT,
    )

    # Topological execution order strictly matching legacy sequences
    validate_env >> submit_pyspark_job