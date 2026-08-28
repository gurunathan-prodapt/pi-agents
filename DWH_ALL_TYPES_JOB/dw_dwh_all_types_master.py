"""
Apache Airflow DAG: dw_dwh_all_types_master

This DAG is converted from the legacy UC4 JOBS_UNIX object 'DW.DWH_ALL_TYPES_MASTER'.
It serves as a showcase pipeline combining Ab Initio, Oracle SQL, KSH, and AWK components
into a single, sequential processing chain. 

The pipeline execution flow is:
1. Primary Processing Task: Runs the PySpark-migrated Ab Initio graph (all_types_graph.mp).
2. Secondary Processing Task: Runs the migrated version of the KSH script (r_all_types_master.ksh).
3. Data Transformation Task: Runs the migrated version of the AWK script (k_all_types_transform.awk).
4. Database Operations Task: Runs the BigQuery-migrated SQL script (d_all_types.sql).
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ==========================================
# GLOBAL GCP INFRASTRUCTURE CONSTANTS
# ==========================================
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ==========================================
# GLOBAL ENVIRONMENT CONSTANTS
# ==========================================
CCR_DIR_ROOT = os.environ.get("CCR_DIR_ROOT")
HOME = os.environ.get("HOME")
ALL_TYPES_DIR_EXP_UTL = Variable.get("ALL_TYPES_DIR_EXP_UTL")

# ==========================================
# JOB-SPECIFIC CONFIGURATION
# ==========================================
JOB_CONFIG = {
    "ALL_TYPES_Projektverzeichnis": "/Projects/TMD/processing/ALL_TYPES/",
    "ALL_TYPES_Graph": "all_types_graph",
    "ALL_TYPES_Version": "RLS_ALL_TYPES_current",
    "ALL_TYPES_Prozesstyp": "N",
    "ALL_TYPES_Datenobjekt": "-",
    "ALL_TYPES_AI_DAT_FILE_DIR": f"{ALL_TYPES_DIR_EXP_UTL}/cubes/at" if ALL_TYPES_DIR_EXP_UTL else None
}

# Define default arguments for the DAG tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_dwh_all_types_master',
    default_args=default_args,
    description='Showcase job combining Ab Initio, Oracle SQL, KSH and AWK components in a single chain',
    schedule=None,                                    # Sourced from Design Document Table (externally triggered)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,                                # Concurrency lock configured at DAG level
    is_paused_upon_creation=False,                     # Sourced from Active=1 (Active on creation)
    template_searchpath=[
        os.path.dirname(__file__),
        "/home/airflow/gcs/dags",
    ],
) as dag:

    # -------------------------------------------------------------------------
    # TASK 1: Primary Processing Task (PySpark Dataproc Submit Job)
    # -------------------------------------------------------------------------
    # This task submits the PySpark job translating the legacy Ab Initio graph 'all_types_graph.mp'.
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/all_types_master.py",
            "args": [
                "--job_kennung", "ALL_TYPES_MASTER",
                "--job_type", "all_types",
                "--key", "all_types_graph",
                "--ccr_dir_root", str(CCR_DIR_ROOT),
                "--all_types_dir_exp_utl", str(ALL_TYPES_DIR_EXP_UTL)
            ]
        }
    }

    all_types_master_graph = DataprocSubmitJobOperator(
        task_id='all_types_master_graph',
        job=pyspark_job_config,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        job_id="{{ dag.dag_id }}_{{ run_id | replace(':', '_') | replace('+', '_') }}_all_types_master_graph",
    )

    # -------------------------------------------------------------------------
    # TASK 2: Secondary Processing Task (r_all_types_master)
    # -------------------------------------------------------------------------
    # Runs the Python translation of the legacy KSH script 'r_all_types_master.ksh'.
    r_all_types_master_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/isall/aufbereitung/bin/r_all_types_master.py",
            "args": [
                "--job_kennung", "ALL_TYPES_MASTER",
                "--ccr_dir_root", str(CCR_DIR_ROOT)
            ]
        }
    }

    r_all_types_master = DataprocSubmitJobOperator(
        task_id='r_all_types_master',
        job=r_all_types_master_config,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        job_id="{{ dag.dag_id }}_{{ run_id | replace(':', '_') | replace('+', '_') }}_r_all_types_master",
    )

    # -------------------------------------------------------------------------
    # TASK 3: Data Transformation Task (k_all_types_transform)
    # -------------------------------------------------------------------------
    # Runs the Python translation of the legacy AWK script 'k_all_types_transform.awk'.
    k_all_types_transform_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/isall/aufbereitung/awk/k_all_types_transform.py",
            "args": [
                "--job_kennung", "ALL_TYPES_MASTER",
                "--ccr_dir_root", str(CCR_DIR_ROOT)
            ]
        }
    }

    k_all_types_transform = DataprocSubmitJobOperator(
        task_id='k_all_types_transform',
        job=k_all_types_transform_config,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        job_id="{{ dag.dag_id }}_{{ run_id | replace(':', '_') | replace('+', '_') }}_k_all_types_transform",
    )

    # -------------------------------------------------------------------------
    # TASK 4: Database Operations Task (d_all_types)
    # -------------------------------------------------------------------------
    # Runs the BigQuery-migrated SQL script 'd_all_types.sql'.
    d_all_types = BigQueryInsertJobOperator(
        task_id='d_all_types',
        configuration={
            "query": {
                "query": "{% include 'isall/aufbereitung/sql/d_all_types.sql' %}",
                "useLegacySql": False,
            }
        },
        project_id=GCP_PROJECT_ID,
    )

    # -------------------------------------------------------------------------
    # TASK DEPENDENCY MAP
    # -------------------------------------------------------------------------
    all_types_master_graph >> r_all_types_master >> k_all_types_transform >> d_all_types