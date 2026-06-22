from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.dataform import DataformRunOperator

# Define your GCP project ID here. This should be consistent across your GCP setup.
# In a production environment, consider using Airflow Variables or Connections.
GCP_PROJECT_ID = "project_id" # Placeholder: Replace with your actual GCP Project ID
REGION = "us-central1"       # Placeholder: Replace with your Dataform repository region
DATAFORM_REPOSITORY_ID = "your-dataform-repository-id" # Placeholder: Replace with your Dataform Repository ID

with DAG(
    dag_id="r_ausd_v_ta_inv_def_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Or set your desired schedule e.g., "@daily"
    catchup=False,
    tags=["isbert", "dataform", "bigquery"],
    params={
        "job_kennung": "BERT_DROP_TEMP_TABLE", # Corresponds to -j $JobKennung in original ksh
        "default_v_datum": "19000101" # Default if no record found for job_kennung
    }
) as dag:
    # Task 1: Fetch v_datum from BigQuery
    # This replaces the Oracle SQLPlus COLUMN s_datum new_value v_datum and SELECT logic
    get_v_datum = BigQueryExecuteQueryOperator(
        task_id="get_v_datum",
        sql=f"""
            SELECT
                COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '{dag.params['default_v_datum']}')
            FROM
                `{GCP_PROJECT_ID}.stg_carmen.dwtk_meldungen` m
            WHERE
                m.job_kennung = '{dag.params['job_kennung']}';
        """,
        use_legacy_sql=False,
        do_xcom_push=True,
        gcp_conn_id="google_cloud_default", # Assumes a Google Cloud connection is configured
    )

    # Task 2: Trigger the Dataform job
    # This executes all actions in the Dataform repository, including the
    # sof_ta_inv_def model and its dependencies (declared staging tables).
    # It passes the dynamically fetched v_datum as a compilation variable.
    run_dataform_job = DataformRunOperator(
        task_id="run_dataform_job",
        project_id=GCP_PROJECT_ID,
        region=REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        # Argument for Dataform compilation.
        # This makes the `v_datum` available as `dataform.projectConfig.vars.v_datum` in SQLX.
        # Ensure the BigQueryExecuteQueryOperator returns a single string result for XCom.
        compilation_variables={
            "v_datum": "{{ task_instance.xcom_pull('get_v_datum')[0][0] }}"
        },
        gcp_conn_id="google_cloud_default",
    )

    # Optional: Add a success notification task
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command="echo 'Dataform job r_ausd_v_ta_inv_def_dag completed successfully!'",
    )

    # Define task dependencies
    get_v_datum >> run_dataform_job >> notify_success

# Additional notes for deployment:
# 1. Ensure your Airflow environment (Cloud Composer) has the necessary
#    permissions to interact with BigQuery and Dataform.
# 2. Replace 'project_id', 'your-dataform-repository-id', and 'us-central1'
#    with your actual GCP project ID, Dataform repository ID, and region.
# 3. The `gcp_conn_id="google_cloud_default"` assumes a default connection
#    is configured in Airflow.
# 4. Ingest data into `project_id.stg_carmen.dwtk_meldungen`,
#    `project_id.stg_carmen.cds_ta_inv_definition`,
#    `project_id.stg_carmen.cds_ta_inv_cont_config`, and
#    `project_id.stg_carmen.cds_ta_care_description` tables
#    before this DAG runs.
# 5. The translation of Oracle (+) joins with WHERE conditions
#    can be complex. The Dataform SQLX reflects a direct translation
#    with `COALESCE` for date conditions and `is_production`.
#    Thorough testing with sample data is crucial to confirm semantic equivalence.