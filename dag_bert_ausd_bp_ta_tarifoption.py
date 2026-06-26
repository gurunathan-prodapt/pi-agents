# -*- coding: utf-8 -*-
# ===================================================================
# Target Platform: Apache Airflow (Google Cloud Composer)
# Legacy Replaced: r_ausd_bp_ta_tarifoption.ksh, k_ausd_bp_ta_tarifoption.ksh,
#                  d_ausd_bp_ta_tarifoption.sql, and UC4 xml
# Job:            DW.BERT_AUSD_BP_TA_TARIFOPTION
# Description:     Orchestrates aggregation of contract tariff options into sof_ta_tarifoption
# ===================================================================

import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime.datetime(2026, 4, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    'dw_bert_ausd_bp_ta_tarifoption',
    default_args=default_args,
    description='Orchestrates aggregation of contract tariff options into sof_ta_tarifoption',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task to run the BigQuery multi-statement script
    run_tarifoption_aggregation = BigQueryInsertJobOperator(
        task_id='run_tarifoption_aggregation',
        configuration={
            "query": {
                "query": """
                    DECLARE v_datum STRING;

                    -- Step 1: Identify the run-date suffix from the metadata table
                    SET v_datum = (
                      SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(timecreated)), '19000101')
                      FROM `target_project.target_dataset.dwtk_meldungen`
                      WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
                    );

                    -- Step 2: Build staging/intermediate table dynamically
                    EXECUTE IMMEDIATE FORMAT('''
                      CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_bpr_opt_filter` AS
                      SELECT
                        t.bpr_id,
                        t.cntrct_id,
                        t.pds_description,
                        l.opt_kategorie
                      FROM
                        `target_project.target_dataset.sof_ta_l_bpr_optionen_filter` l
                      JOIN
                        `target_project.target_dataset.sof_ta_bpr_opt_text_%s` t
                      ON
                        t.bpr_id = l.bpr_id
                    ''', v_datum);

                    -- Step 3: Aggregate and create final target table
                    CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_tarifoption` AS
                    SELECT
                      cntrct_id,
                      SUBSTR(
                        STRING_AGG(
                          IF(opt_kategorie = 'BUDGET', pds_description, NULL), 
                          ', ' ORDER BY pds_description
                        ), 1, 500
                      ) AS business_option,
                      SUBSTR(
                        STRING_AGG(
                          IF(opt_kategorie = 'SONST', pds_description, NULL), 
                          ', ' ORDER BY pds_description
                        ), 1, 500
                      ) AS sonstige_option,
                      SUBSTR(
                        STRING_AGG(
                          IF(opt_kategorie = 'GPRS', pds_description, NULL), 
                          ', ' ORDER BY pds_description
                        ), 1, 500
                      ) AS gprs_option
                    FROM
                      `target_project.target_dataset.sof_ta_bpr_opt_filter`
                    GROUP BY
                      cntrct_id;
                """,
                "useLegacySql": False,
            }
        },
    )

    run_tarifoption_aggregation