# Airflow DAG for DW.BERT_AUSD_V_TA_P_VERTRAG
# Migrated from UC4 Job: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_VERTRAG.xml
# Replaces KornShell scripts and Oracle SQL*Plus.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Define project and dataset variables - REPLACE WITH YOUR ACTUAL VALUES
PROJECT_ID = 'your-gcp-project'
BIGQUERY_DATASET = 'your_bigquery_dataset'
SQL_FILE_PATH = 'sql/d_ausd_v_ta_p_vertrag_bq.sql' # Path to the BigQuery SQL file within the DAGs folder

def _read_sql_file(sql_file_path: str) -> str:
    """Reads the content of an SQL file."""
    # In a real Airflow environment, this file might be in a GCS bucket
    # or part of the DAG deployment. For local execution/testing,
    # assume it's relative to the DAG file or specified via an absolute path.
    # For this exercise, we assume the SQL content can be inline or dynamically loaded.
    # For BigQueryExecuteQueryOperator, the SQL can be provided directly.
    # As per the prompt, the SQL will be part of the generated output.
    # The actual file path for BigQueryExecuteQueryOperator will be relative to DAGs folder,
    # so we will use templating for that.
    
    # For now, we will embed the SQL directly as a string for simplicity in the operator.
    # In a production environment, it's often better to load from a file or GCS.
    # To keep the DAG self-contained and runnable, I will provide the SQL content directly
    # from the generated SQL file.
    
    # This function is not strictly needed if the SQL is embedded,
    # but serves as a placeholder if external file loading is preferred.
    pass

with DAG(
    dag_id="dw_bert_ausd_v_ta_p_vertrag_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None, # Define your schedule, e.g., "@daily", "0 0 * * *"
    catchup=False,
    tags=["bert", "vertrag", "bigquery"],
    description="Airflow DAG for DW.BERT_AUSD_V_TA_P_VERTRAG to update contract information in BigQuery.",
) as dag:
    
    start_processor = PythonOperator(
        task_id="start_python_processor",
        python_callable=__import__('python.p_vertrag_processor').p_vertrag_processor.main,
        # If p_vertrag_processor.py needed arguments, they would be passed via op_kwargs:
        # op_kwargs={'some_param': 'some_value'}
    )

    # The SQL content from the generated sql/d_ausd_v_ta_p_vertrag_bq.sql file
    # This is embedded for self-containment and direct runnability.
    bigquery_sql_content = """
-- Migrated from Oracle SQL: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql
-- Job: DW.BERT_AUSD_V_TA_P_VERTRAG
--
-- This script processes contract data, truncates temporary tables,
-- and populates the primary contract table in BigQuery.

-- Define project and dataset variables - REPLACE WITH YOUR ACTUAL VALUES
DECLARE PROJECT_ID STRING DEFAULT 'your-gcp-project';
DECLARE DATASET_ID STRING DEFAULT 'your_bigquery_dataset';

-- Stichtag ermitteln (Determine key date)
DECLARE v_datum STRING;
SET v_datum = COALESCE((SELECT FORMAT_DATE('%Y%m%d', MAX(m.timecreated)) FROM `your-gcp-project.your_bigquery_dataset.dwtk_meldungen_bq` m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'), '19000101');

-- Leeren der Zieltabelle (Truncate target table)
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_bq`;

-- Insert/Update twin-bill contract data
INSERT INTO `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_bq`
       (vertrag_id_carmen,
       partner_id_carmen,
       rechdef_id_carmen,
       kundenkonto,
       mwst_kennzeichen,
       rahmenvertrag_id,
       rechnungslauf,
       vo_kenn,
       geplant_kuend,
       eingang_kuend,
       vertragsbeginn,
       vertragsstatus,
       sperrart,
       sperrgrund,
       stillegungszeitraum,
       twincard,
       dwh_tarifgr_text,
       bindefrist,
       letztes_upgrade,
       vertragsbindung,
       vertragsbindungseinheit,
       rechnungszahlart,
       rechnungsmedium,
       twin_vertrag_id,
       upgradeberechtigt,
       apn,
       upgradegrund,
       sv_id,
       vda,
       cost_centre,
       cost_centre_user,
       cntrct_ty,
       segment_id,
       rv_action_id,
       rechn_inh_konfig_text,
       order_number,
       commitment_reference_date,
       cntrct_validity_id)
SELECT
       v.vertrag_id_carmen,
       v.partner_id_carmen,
       v.rechdef_id_carmen,
       v.kundenkonto,
       v.mwst_kennzeichen,
       v.rahmenvertrag_id,
       v.rechnungslauf,
       v.vo_kenn,
       v.geplant_kuend,
       v.eingang_kuend,
       v.vertragsbeginn,
       v.vertragsstatus,
       v.sperrart,
       v.sperrgrund,
       v.stillegungszeitraum,
       v.twincard,
       v.dwh_tarifgr_text,
       v.bindefrist,
       v.letztes_upgrade,
       v.vertragsbindung,
       v.vertragsbindungseinheit,
       v.rechnungszahlart,
       v.rechnungsmedium,
       v.twin_vertrag_id,
       v.upgradeberechtigt,
       v.apn,
       v.upgradegrund,
       v.sv_id,
       v.vda,
       v.cost_centre,
       v.cost_centre_user,
       v.cntrct_ty,
       v.segment_id,
       v.rv_action_id,
       v.rechn_inh_konfig_text,
       v.order_number,
       v.commitment_reference_date,
       v.cntrct_validity_id
  FROM
        `your-gcp-project.your_bigquery_dataset.sof_ta_vertrag_tmp_bq` v
  LEFT JOIN
        `your-gcp-project.your_bigquery_dataset.sof_ta_vertrag_tmp_bq` pv
    ON
        v.twin_vertrag_id = pv.vertrag_id_carmen;

-- Leeren der temporaeren Zwischentabellen (Truncate temporary tables)
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_disc_zusgf_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_discount_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_barrier_zusgf_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_barrier_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_cntrct_crs_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_cntrct_templ_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_cntrct_valid_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_period_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_bp_ref_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_inv_assign_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_inv_def_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_acc_ref_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_notice_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_apn_ve_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_discount_rr_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_vvl_dwh_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_vvl_upgrade_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_cntrct_crs2_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_cntrct_crs3_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_inv_acc_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_vertrag_tmp_bq`;
TRUNCATE TABLE `your-gcp-project.your_bigquery_dataset.sof_ta_action_assoc_bq`;
"""

    execute_bigquery_transformation = BigQueryExecuteQueryOperator(
        task_id="execute_bigquery_transformation",
        sql=bigquery_sql_content,
        use_legacy_sql=False, # Use Standard SQL
        location="EU", # Specify your BigQuery dataset location
        gcp_conn_id="google_cloud_default", # Ensure this connection is configured in Airflow
        # The project_id for the query is derived from the connection or environment,
        # but can also be explicitly set if needed.
        # project_id=PROJECT_ID
    )

    # Define task dependencies
    start_processor >> execute_bigquery_transformation