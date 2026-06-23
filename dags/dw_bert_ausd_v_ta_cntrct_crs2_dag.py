# Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_CNTRCT_CRS2.xml
# Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS2

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.utils.trigger_rule import TriggerRule

BIGQUERY_CONN_ID = "google_cloud_default"
# GCP Project ID is typically dynamically configured in Airflow,
# or can be set directly if immutable. Using Airflow variable for flexibility.
# Ensure 'gcp_project' variable is set in your Airflow environment.
PROJECT_ID = "{{ var.value.gcp_project }}"
DATASET_ID = "dw_bert_staging" # As specified in the design document

with DAG(
    dag_id="dw_bert_ausd_v_ta_cntrct_crs2_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set to your desired schedule (e.g., "@daily", "0 0 * * *")
    catchup=False,
    tags=["bert", "bigquery", "contract"],
    description="Updates contract information in sof_ta_cntrct_crs2, excluding frame contract parents.",
) as dag:
    # Task to create the target table if it doesn't exist
    # The DDL is directly embedded, referencing the PROJECT_ID and DATASET_ID
    # for full table path.
    create_target_table_if_not_exists = BigQueryOperator(
        task_id="create_sof_ta_cntrct_crs2_table_if_not_exists",
        bigquery_conn_id=BIGQUERY_CONN_ID,
        use_legacy_sql=False,
        sql=f"""
            -- Legacy Source: Implied schema from INSERT statement in d_ausd_v_ta_cntrct_crs2.sql
            -- Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS2

            CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_crs2` (
                cntrct_id INT64,
                obj_version INT64,
                contract_number STRING,
                cntrct_template_id INT64,
                cntrct_validity_id INT64,
                valid_from DATE,
                com_per_ext_rea_cv STRING,
                billcycle_id INT64,
                vo_code STRING,
                cntrct_start_date DATE,
                cntrct_st INT64,
                cntrct_parent INT64,
                cntrct_ty INT64,
                cost_centre STRING,
                cost_centre_user STRING,
                commitment_reference_date DATE,
                order_number STRING,
                rv_num STRING
            );
        """,
    )

    # Task to truncate and load data into the target table
    # The SQL query for this task is loaded from an external file for better maintainability.
    # The file path is relative to the DAGs folder or configured through Airflow's file system.
    load_contract_data = BigQueryOperator(
        task_id="load_sof_ta_cntrct_crs2_data",
        bigquery_conn_id=BIGQUERY_CONN_ID,
        use_legacy_sql=False,
        # The SQL file needs to be accessible by Airflow, e.g., in the 'sql' folder relative to the DAG.
        sql=[
            f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_crs2`;",
            f"""
            -- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs2.sql
            -- Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS2

            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_crs2` (
                    cntrct_id,
                    obj_version,
                    contract_number,
                    cntrct_template_id,
                    cntrct_validity_id,
                    valid_from,
                    com_per_ext_rea_cv,
                    billcycle_id,
                    vo_code,
                    cntrct_start_date,
                    cntrct_st,
                    cntrct_parent,
                    cntrct_ty,
                    cost_centre,
                    cost_centre_user,
                    commitment_reference_date,
                    order_number,
                    rv_num
            )
            SELECT
                    c.cntrct_id,
                    c.obj_version,
                    c.contract_number,
                    c.cntrct_template_id,
                    c.cntrct_validity_id,
                    c.valid_from,
                    c.com_per_ext_rea_cv,
                    c.billcycle_id,
                    c.vo_code,
                    c.cntrct_start_date,
                    c.cntrct_st,
                    c.cntrct_parent,
                    c.cntrct_ty,
                    c.cost_centre,
                    c.cost_centre_user,
                    c.commitment_reference_date,
                    c.order_number,
                    cr.contract_number AS RV_NUM
            FROM
                    `{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_crs` AS c
            LEFT OUTER JOIN
                    `{PROJECT_ID}.{DATASET_ID}.sof_ta_cntrct_crs` AS cr
            ON
                    c.cntrct_parent = cr.cntrct_id
                AND
                    cr.cntrct_ty = 10 -- RV
            WHERE
                    c.cntrct_ty <> 10;
            """
        ],
    )

    create_target_table_if_not_exists >> load_contract_data