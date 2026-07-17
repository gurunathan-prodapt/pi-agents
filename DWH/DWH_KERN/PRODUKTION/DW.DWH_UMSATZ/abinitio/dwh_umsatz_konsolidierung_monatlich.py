"""
DAG: dwh_umsatz_konsolidierung_monatlich
Description: Orchestrates monthly consolidation of revenue streams and performs quality checks.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator, BigQueryCheckOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.exceptions import AirflowFailException

# --- Fetching Environment Context & Orchestration Variables ---
GCP_PROJECT = Variable.get("gcp_project")
GCP_REGION = Variable.get("gcp_region", default_var="europe-west3")
GCS_BUCKET = Variable.get("gcs_bucket")
BQ_DATASET_STG = Variable.get("bq_dataset_stg", default_var="DWH_STAGING")
BQ_DATASET_DWH = Variable.get("bq_dataset_dwh", default_var="DWH_CORE")

# Run-time execution parameters (Can be set via manual trigger or derived)
VERARBEITUNGSMONAT = "{{ dag_run.conf.get('verarbeitungsmonat', ds_nodash[:6]) }}"
KONZERNGESELLSCHAFT = "{{ dag_run.conf.get('konzerngesellschaft', 'COMPANY_DE') }}"

# Quality parameters
MIN_ROW_COUNT = 1
KONSOLIDIERUNG_TOLERANZ = 2.5  # Percentage threshold
MAX_ABWEICHUNGEN = 25

default_args = {
    "owner": "DWH-Data-Platform-Team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["dwh-alerts@yourdomain.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dwh_umsatz_konsolidierung_monatlich",
    default_args=default_args,
    description="Monthly revenue aggregation and DWH validation pipeline.",
    schedule_interval="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Validation Sensor: Ensure execution month exists and is set to active
    validate_period_sensor = BigQueryValueCheckOperator(
        task_id="validate_period_sensor",
        sql=f"""
            SELECT status 
            FROM `{GCP_PROJECT}.{BQ_DATASET_DWH}.DIM_PROCESS_PERIODS`
            WHERE period_id = '{VERARBEITUNGSMONAT}'
        """,
        pass_value="ACTIVE",
        use_legacy_sql=False,
    )

    # 2. PySpark execution on Dataproc Serverless
    submit_pyspark_job = DataprocCreateBatchOperator(
        task_id="submit_pyspark_job",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        batch_id=f"umsatz-kons-{KONZERNGESELLSCHAFT.lower()}-{VERARBEITUNGSMONAT}-" 
                 f"{{{{ ts_nodash | lower }}}}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{GCS_BUCKET}/src/umsatz_konsolidierung.py",
                "args": [
                    "--verarbeitungsmonat", VERARBEITUNGSMONAT,
                    "--konzerngesellschaft", KONZERNGESELLSCHAFT,
                    "--gcs_bucket", GCS_BUCKET,
                    "--bq_dataset_stg", f"{GCP_PROJECT}.{BQ_DATASET_STG}",
                    "--bq_dataset_dwh", f"{GCP_PROJECT}.{BQ_DATASET_DWH}"
                ],
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": "default"
                }
            }
        }
    )

    # 3. Row Count Verification: Ensure data has been written
    validate_row_counts = BigQueryValueCheckOperator(
        task_id="validate_row_counts",
        sql=f"""
            SELECT COUNT(1) 
            FROM `{GCP_PROJECT}.{BQ_DATASET_DWH}.FACT_UMSATZ_KONS_MONAT`
            WHERE verarbeitungsmonat = '{VERARBEITUNGSMONAT}'
              AND konzerngesellschaft = '{KONZERNGESELLSCHAFT}'
        """,
        pass_value=MIN_ROW_COUNT,
        use_legacy_sql=False,
        relationship=">=",
    )

    # 4. Statistical Validation (Toleranzpruefung)
    # Checks deviations from the previous month. If variations exceed tolerances, the task fails.
    check_konsolidierung_toleranz = BigQueryCheckOperator(
        task_id="check_konsolidierung_toleranz",
        sql=f"""
            WITH cur_period AS (
              SELECT SUM(umsatz_cents) as total_cur
              FROM `{GCP_PROJECT}.{BQ_DATASET_DWH}.FACT_UMSATZ_KONS_MONAT`
              WHERE verarbeitungsmonat = '{VERARBEITUNGSMONAT}'
                AND konzerngesellschaft = '{KONZERNGESELLSCHAFT}'
            ),
            prev_period AS (
              SELECT SUM(umsatz_cents) as total_prev
              FROM `{GCP_PROJECT}.{BQ_DATASET_DWH}.FACT_UMSATZ_KONS_MONAT`
              WHERE verarbeitungsmonat = CAST(CAST('{VERARBEITUNGSMONAT}' AS INT64) - 1 AS STRING)
                AND konzerngesellschaft = '{KONZERNGESELLSCHAFT}'
            )
            SELECT 
              CASE 
                WHEN ABS(COALESCE(cur.total_cur, 0) - COALESCE(prev.total_prev, 0)) / 
                     NULLIF(COALESCE(prev.total_prev, 0), 0) * 100 > {KONSOLIDIERUNG_TOLERANZ} 
                     AND ABS(COALESCE(cur.total_cur, 0) - COALESCE(prev.total_prev, 0)) > {MAX_ABWEICHUNGEN}
                THEN FALSE
                ELSE TRUE
              END AS toleranz_bestaetigt
            FROM cur_period cur, prev_period prev
        """,
        use_legacy_sql=False,
    )

    # --- Task Pipeline Order ---
    validate_period_sensor >> submit_pyspark_job >> validate_row_counts >> check_konsolidierung_toleranz