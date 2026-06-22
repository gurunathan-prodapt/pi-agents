# Airflow DAG for DW.BERT_AUSD_V_TA_P_DISCOUNT_RR
# Replaces: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

with DAG(
    dag_id='dw_bert_ausd_v_ta_p_discount_rr',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Schedule will be determined based on business requirements
    catchup=False,
    tags=["bert", "dw", "etl"],
    description="ETL workflow to enrich discount data with contract information.",
) as dag:
    start_task = DummyOperator(task_id="start")

    # This task submits a PySpark job to a Dataproc cluster.
    # The PySpark script (r_ausd_v_ta_p_discount_rr.py) will execute the BigQuery SQL.
    # TODO: Replace 'your-gcp-project-id', 'us-central1', 'bert-dataproc-cluster',
    # and 'your-gcs-bucket' with actual values for your GCP environment.
    run_transformation_task = DataprocSubmitJobOperator(
        task_id="run_pyspark_transformation",
        project_id="your-gcp-project-id",
        region="us-central1",
        cluster_name="bert-dataproc-cluster",
        job={
            "placement": {"cluster_name": "bert-dataproc-cluster"},
            "pyspark_job": {
                "main_python_file_uri": "gs://your-gcs-bucket/pyspark_scripts/r_ausd_v_ta_p_discount_rr.py",
                # The BigQuery SQL script is provided as a file_uri to be accessible by the PySpark job.
                "file_uris": ["gs://your-gcs-bucket/sql_scripts/d_ausd_v_ta_p_discount_rr.sql.bq"]
            },
        },
    )

    end_task = DummyOperator(task_id="end")

    start_task >> run_transformation_task >> end_task