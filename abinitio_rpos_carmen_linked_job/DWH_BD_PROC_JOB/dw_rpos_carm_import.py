"""
DAG for RPOS Carmen Import.
This DAG orchestrates the import of Carmen-related retail point of sale (RPOS) billing data.
It migrates the UC4 UNIX job DW.RPOS_CARM_IMPORT to run as a PySpark application
on Google Cloud Dataproc.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GLOBAL CONFIGURATION (ENVIRONMENT-WIDE INFRASTRUCTURE) ───────────────────
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")
DW_DIR_IMP_SAP = Variable.get("DW_DIR_IMP_SAP")

# ── DEFAULT ARGUMENTS ────────────────────────────────────────────────────────
DEFAULT_ARGS = { 
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_rpos_carm_import",
    default_args=DEFAULT_ARGS,
    description="Job startet AbInitio Graph map_rpos_carmen_import",
    schedule_interval=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Task: rpos_carm_import ───────────────────────────────────────────────
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/map_rpos_carmen_import.py",
            "properties": {
                "spark.yarn.appMasterEnv.DWH_JOB_KENNUNG": "RPOS_CARM_IMPORT",
                "spark.yarn.appMasterEnv.BHB_Projektverzeichnis": "/Projects/TMD/processing/BHB/BD_PROC",
                "spark.yarn.appMasterEnv.BHB_Version": "RLS_BHB_nach_64_rabatt_sap",
                "spark.yarn.appMasterEnv.BHB_Graph": "map_rpos_carmen_import",
                "spark.yarn.appMasterEnv.BHB_Prozesstyp": "D",
                "spark.yarn.appMasterEnv.BHB_Quellverzeichnis": f"{DW_DIR_IMP_SAP}/crs/work/",
                "spark.yarn.appMasterEnv.BHB_Zielverzeichnis": f"{DW_DIR_IMP_SAP}/crs/store/",
                "spark.yarn.appMasterEnv.BHB_Dateimaske": "CARMEN_B_*_pos.fix",
                "spark.yarn.appMasterEnv.BHB_Kopfdatensatzkennung": "H",
                "spark.yarn.appMasterEnv.BHB_Nutzdatensatzkennung": "P",
                "spark.yarn.appMasterEnv.BHB_Endedatensatzkennung": "X",
            }
        }
    }

    rpos_carm_import = DataprocSubmitJobOperator(
        task_id="rpos_carm_import",
        job=pyspark_job_config,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
    )

    rpos_carm_import