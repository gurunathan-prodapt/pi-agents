"""
DAG ID: dw_dwh_dummy_absd_plato_tarife

Overview:
This DAG is a migration of the UC4 JOBS_UNIX object 'DW.DWH_DUMMY_ABSD_PLATO_TARIFE'.
In UC4, this is a "dummy" or placeholder step in the larger 'DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP' workflow.
The original script contains only a print directive (:print Doing nothinig) and no active 
system or application commands. 

To replicate this administrative/sync-point behavior resource-efficiently, this DAG 
recommends using an EmptyOperator (dwh_dummy_absd_plato_tarife). An alternative 
DataprocSubmitJobOperator block is provided in comments to comply with rigid 
GCP Dataproc execution frameworks if required.

Schedule: None (Designed to be triggered manually or orchestrated by a parent pipeline)
"""

# ── Imports ──────────────────────────────────────────────
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta

# ── GCP Configuration ────────────────────────────────────
# Standard environment configuration sourced from Airflow Variables
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── Default Args ─────────────────────────────────────────
# Default configuration mapping source specifications
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),  # Aligned to UC4 metadata timestamps
    "retries": 0,                         # Source has no automatic restart defined
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=DEFAULT_ARGS,
    description="Migrated dummy task from UC4 DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
    schedule=None,                        # No schedule available in source files
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,        # Source active state: <Active>1</Active>
) as dag:

    # ── Task Definitions ─────────────────────────────────────

    # Master start gate
    start = EmptyOperator(
        task_id="start"
    )

    # OPTION 1 (RECOMMENDED): Target task as an EmptyOperator (No execution footprint)
    # Perfectly replicates the "Doing nothing" behavior of the source UC4 script.
    dwh_dummy_absd_plato_tarife = EmptyOperator(
        task_id="dwh_dummy_absd_plato_tarife",
        doc_md="""
        ### UC4 Job Documentation
        Wiederanlauf ohne weitere Maßnahmen möglich.
        
        *Source XML:* `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`
        *Action:* No-op (Original script contained only `:print Doing nothinig`).
        """
    )

    # OPTION 2 (ALTERNATIVE): Dataproc configuration (Commented out)
    # To use Option 2, uncomment this block and update the dependency chain.
    """
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"
        }
    }

    dwh_dummy_absd_plato_tarife_alt = DataprocSubmitJobOperator(
        task_id="dwh_dummy_absd_plato_tarife_alt",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job=pyspark_job_config,
        # Dynamic Job ID derivation to avoid duplicate ID rejections
        job_id="{{ dag.dag_id }}_{{ run_id }}_dwh_dummy_absd_plato_tarife_alt",
    )
    """

    # Master end gate
    end = EmptyOperator(
        task_id="end"
    )

    # ── Dependencies ─────────────────────────────────────────
    # Simple linear execution mapping for a single node pipeline
    start >> dwh_dummy_absd_plato_tarife >> end