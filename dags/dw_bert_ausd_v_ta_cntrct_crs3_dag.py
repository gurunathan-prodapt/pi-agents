# Airflow DAG for DW.BERT_AUSD_V_TA_CNTRCT_CRS3
# Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/.../DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml
# Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS3
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dw_bert_ausd_v_ta_cntrct_crs3",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,  # The schedule for this DAG should be determined by further UC4 analysis (EVNT_TIME).
    tags=["bert", "dw", "contract", "bigquery"],
    doc_md="""
    ### DW.BERT_AUSD_V_TA_CNTRCT_CRS3

    This DAG migrates the legacy UC4, KornShell, and Oracle SQL job `DW.BERT_AUSD_V_TA_CNTRCT_CRS3`
    to Google Cloud Platform using BigQuery and Airflow.
    It is responsible for updating contract data, including twin-bill information,
    into the `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` BigQuery table.

    The DAG executes a Python script (`contract_data_updater.py`) which, in turn,
    runs the converted BigQuery SQL (`d_ausd_v_ta_cntrct_crs3_bq.sql`).

    **Legacy Components Replaced:**
    - `DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml` (UC4 Job Definition)
    - `r_ausd_v_ta_cntrct_crs3.ksh` (KornShell wrapper)
    - `k_ausd_v_ta_cntrct_crs3.ksh` (KornShell control script)
    - `d_ausd_v_ta_cntrct_crs3.sql` (Oracle SQL transformation)

    **Dependencies:**
    - BigQuery tables: `my-gcp-project.isbert_schema.dwtk_meldungen`,
      `my-gcp-project.sof_schema.sof_ta_cntrct_crs2`,
      `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`.
    - Python script: `contract_data_updater.py` must be accessible.
    - BigQuery SQL file: `d_ausd_v_ta_cntrct_crs3_bq.sql` must be accessible.
    """,
) as dag:
    # Define the subdirectory where supporting scripts and SQL are located relative to the DAG file.
    scripts_sub_dir = "scripts/dw_bert_ausd_v_ta_cntrct_crs3"
    
    # Placeholder for your GCP Project ID. Replace with your actual value.
    gcp_project_id = "my-gcp-project"

    run_contract_data_update_task = BashOperator(
        task_id="run_contract_data_updater",
        bash_command=f"""
            # Ensure Python and necessary libraries (google-cloud-bigquery) are available in the Airflow environment.
            # The script and SQL file are assumed to be in the '{scripts_sub_dir}' subdirectory relative to the DAG.
            python {scripts_sub_dir}/contract_data_updater.py \\
                --project_id {gcp_project_id} \\
                --sql_file {scripts_sub_dir}/d_ausd_v_ta_cntrct_crs3_bq.sql
        """,
        # You might want to add retries, timeout, and other standard Airflow task parameters.
    )