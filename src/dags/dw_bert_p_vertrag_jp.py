# Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_P_VERTRAG_JP.xml
# Job Plan: DW.BERT_P_VERTRAG_JP

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from callbacks import on_terminal_failure

# GCP Configuration (Replace with actual environment targets or Airflow variables)
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"
PYSPARK_BASE_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts"

default_args = {
    "owner": "uc4_migration",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="dw_bert_p_vertrag_jp",
    schedule=None,  # Configured as manual trigger / external wait until scheduler logic integrated
    catchup=False,
    max_active_runs=1,  # Resolves Else=Wait sync constraint (DW.BERT_P_VERTRAG_JP_SYNC and DW.BERT_BFC_JP_SYNC)
    default_args=default_args,
    tags=["uc4", "bert", "production", "dwh"],
) as dag:

    # Guard node
    guard_active_run = EmptyOperator(task_id="guard_active_run")

    # Helper function to build standard Dataproc PySpark submit operators
    def build_pyspark_task(task_id: str, script_name: str, retries: int = 0) -> DataprocSubmitJobOperator:
        task_callback = on_terminal_failure if retries > 0 else None
        return DataprocSubmitJobOperator(
            task_id=task_id,
            region=DATAPROC_REGION,
            project_id=GCP_PROJECT_ID,
            retries=retries,
            retry_delay=timedelta(minutes=15),
            on_failure_callback=task_callback,
            job={
                "reference": {"project_id": GCP_PROJECT_ID},
                "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
                "pyspark_job": {
                    "main_python_file_uri": f"{PYSPARK_BASE_URI}/{script_name}"
                },
            }
        )

    # Individual pipeline tasks
    task_period = build_pyspark_task(
        "task_period", "dw_bert_ausd_v_ta_period.py"
    )
    
    # Retry task (10 retries, 15m delay as specified in recovery loop)
    task_discount_rr = build_pyspark_task(
        "task_discount_rr", "dw_bert_ausd_v_ta_discount_rr.py", retries=10
    )
    
    task_cntrct_valid = build_pyspark_task(
        "task_cntrct_valid", "dw_bert_ausd_v_ta_cntrct_valid.py"
    )
    
    task_barrier = build_pyspark_task(
        "task_barrier", "dw_bert_ausd_v_ta_barrier.py"
    )
    
    task_vvl_dwh = build_pyspark_task(
        "task_vvl_dwh", "dw_bert_ausd_v_ta_vvl_dwh.py"
    )
    
    task_inv_assign = build_pyspark_task(
        "task_inv_assign", "dw_bert_ausd_v_ta_inv_assign.py"
    )
    
    task_inv_def = build_pyspark_task(
        "task_inv_def", "dw_bert_ausd_v_ta_inv_def.py"
    )
    
    task_acc_ref = build_pyspark_task(
        "task_acc_ref", "dw_bert_ausd_v_ta_acc_ref.py"
    )
    
    task_action_assoc = build_pyspark_task(
        "task_action_assoc", "dw_bert_ausd_v_ta_action_assoc.py"
    )
    
    task_discount = build_pyspark_task(
        "task_discount", "dw_bert_ausd_v_ta_discount.py"
    )
    
    task_apn_ve = build_pyspark_task(
        "task_apn_ve", "dw_bert_ausd_v_ta_apn_ve.py"
    )
    
    task_bp_ref = build_pyspark_task(
        "task_bp_ref", "dw_bert_ausd_v_ta_bp_ref.py"
    )
    
    task_notice = build_pyspark_task(
        "task_notice", "dw_bert_ausd_v_ta_notice.py"
    )
    
    task_cntrct_crs = build_pyspark_task(
        "task_cntrct_crs", "dw_bert_ausd_v_ta_cntrct_crs.py"
    )
    
    task_barrier_zusgf = build_pyspark_task(
        "task_barrier_zusgf", "dw_bert_ausd_v_ta_barrier_zusgf.py"
    )
    
    task_vvl_upgrade = build_pyspark_task(
        "task_vvl_upgrade", "dw_bert_ausd_v_ta_vvl_upgrade.py"
    )
    
    task_inv_acc = build_pyspark_task(
        "task_inv_acc", "dw_bert_ausd_v_ta_inv_acc.py"
    )
    
    task_disc_zusgf = build_pyspark_task(
        "task_disc_zusgf", "dw_bert_ausd_v_ta_disc_zusgf.py"
    )
    
    task_p_discount = build_pyspark_task(
        "task_p_discount", "dw_bert_ausd_v_ta_p_discount.py"
    )
    
    # Retry task (10 retries, 15m delay as specified in recovery loop)
    task_p_discount_rr = build_pyspark_task(
        "task_p_discount_rr", "dw_bert_ausd_v_ta_p_discount_rr.py", retries=10
    )
    
    task_cntrct_crs3 = build_pyspark_task(
        "task_cntrct_crs3", "dw_bert_ausd_v_ta_cntrct_crs3.py"
    )
    
    task_vertrag_tmp = build_pyspark_task(
        "task_vertrag_tmp", "dw_bert_ausd_v_ta_vertrag_tmp.py"
    )
    
    task_p_vertrag = build_pyspark_task(
        "task_p_vertrag", "dw_bert_ausd_v_ta_p_vertrag.py"
    )

    end_node = EmptyOperator(task_id="end")

    # Linearized dependencies matching UC4 job plan execution paths
    (
        guard_active_run
        >> task_period
        >> task_discount_rr
        >> task_cntrct_valid
        >> task_barrier
        >> task_vvl_dwh
        >> task_inv_assign
        >> task_inv_def
        >> task_acc_ref
        >> task_action_assoc
        >> task_discount
        >> task_apn_ve
        >> task_bp_ref
        >> task_notice
        >> task_cntrct_crs
        >> task_barrier_zusgf
        >> task_vvl_upgrade
        >> task_inv_acc
        >> task_disc_zusgf
        >> task_p_discount
        >> task_p_discount_rr
        >> task_cntrct_crs3
        >> task_vertrag_tmp
        >> task_p_vertrag
        >> end_node
    )