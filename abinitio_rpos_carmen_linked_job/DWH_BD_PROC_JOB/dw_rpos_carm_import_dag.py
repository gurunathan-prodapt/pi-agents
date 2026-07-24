from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── Dynamic Environment Variables ────────────────────────
# Global dynamic configurations loaded from Airflow Variable Store
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── Job-Specific Parameters ──────────────────────────────
JOB_CONFIG = {
    "DWH_JOB_KENNUNG": "RPOS_CARM_IMPORT",
    "BHB_Projektverzeichnis": "/Projects/TMD/processing/BHB/BD_PROC",
    "BHB_Version": "RLS_BHB_nach_64_rabatt_sap",
    "BHB_Graph": "map_rpos_carmen_import",
    "BHB_Prozesstyp": "D",
    "BHB_Quellverzeichnis": f"gs://{GCS_BUCKET}/crs/work/",
    "BHB_Zielverzeichnis": f"gs://{GCS_BUCKET}/crs/store/",
    "BHB_Dateimaske": "CARMEN_B_*_pos.fix",
    "BHB_Kopfdatensatzkennung": "H",
    "BHB_Nutzdatensatzkennung": "P",
    "BHB_Endedatensatzkennung": "X",
}

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
# Mirrored Orchestration of DW.RPOS_CARM_IMPORT
with DAG(
    dag_id="dw_rpos_carm_import",
    default_args=DEFAULT_ARGS,
    description="Job startet AbInitio Graph  map_rpos_carmen_import", # OUTPUT/PRINT LITERAL RULE: Verbatim German title preserved
    schedule_interval=None,
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=["dwh", "abinitio", "rpos", "carmen"],
) as dag:

    # ── Task: rpos_carm_import ───────────────────────────
    # Submits the migrated PySpark script representing map_rpos_carmen_import.mp
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/map_rpos_carmen_import.py",
            "args": [
                "--job_kennung", JOB_CONFIG["DWH_JOB_KENNUNG"],
                "--project_dir", JOB_CONFIG["BHB_Projektverzeichnis"],
                "--version", JOB_CONFIG["BHB_Version"],
                "--graph", JOB_CONFIG["BHB_Graph"],
                "--process_type", JOB_CONFIG["BHB_Prozesstyp"],
                "--source_dir", JOB_CONFIG["BHB_Quellverzeichnis"],
                "--target_dir", JOB_CONFIG["BHB_Zielverzeichnis"],
                "--file_mask", JOB_CONFIG["BHB_Dateimaske"],
                "--header_id", JOB_CONFIG["BHB_Kopfdatensatzkennung"],
                "--data_id", JOB_CONFIG["BHB_Nutzdatensatzkennung"],
                "--trailer_id", JOB_CONFIG["BHB_Endedatensatzkennung"]
            ],
        },
        "labels": {
            "uc4_object_name": "dw_rpos_carm_import",
            "job_kennung": "rpos_carm_import"
        }
    }

    rpos_carm_import = DataprocSubmitJobOperator(
        task_id="rpos_carm_import",
        job=pyspark_job_config,
        region=GCP_REGION,
        project_id=GCP_PROJECT_ID,
    )

    rpos_carm_import