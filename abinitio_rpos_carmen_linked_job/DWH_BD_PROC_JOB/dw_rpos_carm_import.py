from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Retrieve Environment Globals
GCP_PROJECT = Variable.get("gcp_project")
GCP_REGION = Variable.get("gcp_region")
GCS_BUCKET = Variable.get("gcs_bucket")
BQ_DATASET = Variable.get("bq_dataset")
SPARK_SERVICE_ACCOUNT = Variable.get("spark_service_account")

# Job-specific variables derived from configuration parameters
PYSPARK_SCRIPT = f"gs://{GCS_BUCKET}/abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.py"
CFG_FILE_PATH = f"gs://{GCS_BUCKET}/abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import.json"
SHARED_AI_START = f"gs://{GCS_BUCKET}/abinitio_pyspark_linked_job/isccr/abinitio/bin/r_ai_start.py"

default_args = {
    "owner": "DWH",
    "start_date": datetime(2026, 1, 1),
    "depends_on_past": False,
}

with DAG(
    dag_id="dw_rpos_carm_import",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["abinitio", "pyspark", "carmen"],
) as dag:

    # Dataproc Serverless Batch Operator executing the PySpark pipeline
    submit_pyspark_job = DataprocCreateBatchOperator(
        task_id="submit_pyspark_job",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        batch_id="dw-rpos-carm-import-batch",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": PYSPARK_SCRIPT,
                "args": [
                    "--cfg_path", CFG_FILE_PATH,
                ],
                "python_file_uris": [
                    SHARED_AI_START
                ]
            },
            "environment_config": {
                "execution_config": {
                    "service_account": SPARK_SERVICE_ACCOUNT,
                }
            }
        }
    )

    # BigQuery Insert Job Operator executing audit updates for dwh_ta_k_meldungen
    update_meldungen_table = BigQueryInsertJobOperator(
        task_id="update_meldungen_table",
        configuration={
            "query": {
                "query": f"""
                    MERGE INTO `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_k_meldungen` t
                    USING `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_k_meldungen_stage` s
                    ON t.entrynr = s.eintragsnr
                    WHEN MATCHED THEN UPDATE SET
                      anzahl_ds_eof = s.anzahl,
                      dateiname = s.bemerkung,
                      enderecord_text = s.inhalt,
                      zusatzinfo = s.bemerkung
                    WHEN NOT MATCHED THEN INSERT (entrynr, anzahl_ds_eof, dateiname, enderecord_text, zusatzinfo)
                      VALUES (s.eintragsnr, s.anzahl, s.bemerkung, s.inhalt, s.bemerkung)
                """,
                "useLegacySql": False,
            }
        }
    )

    # BigQuery Insert Job Operator executing audit updates for dwh_ta_k_rech_absgrp
    update_rech_absgrp_table = BigQueryInsertJobOperator(
        task_id="update_rech_absgrp_table",
        configuration={
            "query": {
                "query": f"""
                    MERGE INTO `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_k_rech_absgrp` t
                    USING `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_k_rech_absgrp_stage` s
                    ON t.monats_id = s.monats_id
                      AND t.abs_grp = s.abs_grp
                      AND t.dateiname = s.dateiname
                      AND t.rechnungsteil = s.rechnungsteil
                    WHEN MATCHED THEN UPDATE SET
                      rechnung_datum = s.rechnung_datum,
                      ladedatum = s.ladedatum
                    WHEN NOT MATCHED THEN INSERT (monats_id, abs_grp, dateiname, rechnung_datum, rechnungsteil, ladedatum)
                      VALUES (s.monats_id, s.abs_grp, s.dateiname, s.rechnung_datum, s.rechnungsteil, s.ladedatum)
                """,
                "useLegacySql": False,
            }
        }
    )

    submit_pyspark_job >> [update_meldungen_table, update_rech_absgrp_table]