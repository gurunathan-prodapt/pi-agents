# Apache Airflow DAG for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
#
# This DAG replaces the KornShell orchestration script and executes
# the transformed BigQuery SQL.
#
# Configuration:
# - Ensure 'gcp_connection_id' is set up in Airflow for your GCP project.
# - The BigQuery dataset IDs `isbert_source_dataset` and `target_dataset`
#   should be replaced with your actual dataset names if different from the default.
# - The 'project_id' placeholder should be updated to your GCP Project ID.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="bert_ausd_v_ta_vvl_dwh",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule here, e.g., "@daily", "0 0 * * *"
    catchup=False,
    tags=["bert", "isbert", "vvl", "bigquery"],
    params={
        "p_jobkennung": "DEFAULT_JOB",  # Corresponds to p_JobKennung from ksh script
        "p_eintragsnr": "DEFAULT_ENTRY",  # Corresponds to p_EintragsNr from ksh script
        "gcp_project_id": "project_id",  # Replace with your GCP project ID
        "isbert_source_dataset": "isbert_source_dataset", # Replace with actual dataset name
        "target_dataset": "target_dataset", # Replace with actual dataset name
    },
) as dag:
    start_task = DummyOperator(
        task_id="start",
    )

    # Task to set up parameters and simulate environment initialization
    # In a real-world scenario, you might have more complex setup here,
    # e.g., fetching parameters from a database or a configuration service.
    # The parameters are passed to the DAG via the 'params' dictionary.
    # We can also log them.
    log_params_task = BashOperator(
        task_id="log_parameters",
        bash_command=(
            "echo 'Starting BERT_AUSD_V_TA_VVL_DWH job with parameters:' && "
            "echo 'p_JobKennung: {{ params.p_jobkennung }}' && "
            "echo 'p_EintragsNr: {{ params.p_eintragsnr }}'"
        ),
    )

    # BigQuery task to execute the transformed SQL script
    # This replaces the `starteSQLSkript` call from the ksh script.
    execute_bigquery_sql = BigQueryExecuteQueryOperator(
        task_id="execute_d_ausd_v_ta_vvl_dwh_sql",
        sql="""
            -- Migrated SQL from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_vvl_dwh.sql
            -- Original Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
            --
            -- This script has been translated to BigQuery Standard SQL.
            -- Placeholder dataset names 'project_id.isbert_source_dataset' and 'project_id.target_dataset' are used.
            -- Please review and replace with actual project and dataset IDs.
            --
            -- Original Oracle SQL comments and SQL*Plus specific commands have been removed or commented out.
            --
            -- TO DO:
            -- 1. Review and refine column data types for all tables based on source system's exact schema.
            -- 2. The PL/SQL package call 'isbert_schema.DWPA_UTIL_SKRIPT.runstatement' needs to be re-implemented.
            --    For TRUNCATE, use DELETE FROM `project_id.target_dataset.sof_ta_vvl_dwh` WHERE TRUE;
            --    The 'runstatement' method's full functionality needs to be analyzed and recreated in BigQuery UDFs/Stored Procedures or Airflow Python tasks.
            -- 3. Parameter handling (e.g., v_datum) from the original script should be managed by Airflow.
            -- 4. Error handling and logging (`DWMSG_MeldeFehler`) should be handled by Airflow's native mechanisms.

            -- Truncate equivalent in BigQuery (DELETE all rows)
            DELETE FROM `{{ params.gcp_project_id }}.{{ params.target_dataset }}.sof_ta_vvl_dwh`
            WHERE TRUE;

            -- Insert data into target table
            INSERT INTO `{{ params.gcp_project_id }}.{{ params.target_dataset }}.sof_ta_vvl_dwh`(
               stichtag,
               vertrags_id,
               dwh_vertrag_id,
               vo_kenn,
               rahmenvertrag,
               dwh_tarifgr_id,
               aenderung_am,
               vvl_aendgrund_id,
               vvl_crd_alt,
               vvl_ersteperiode_alt,
               vvl_folgeperiode_alt,
               vertragsbindedatum_alt,
               vvl_crd_neu,
               vvl_ersteperiode_neu,
               vvl_folgeperiode_neu,
               vertragsbindedatum_neu,
               vertragsbeginn,
               ladedatum,
               vo_kenn_bearb,
               vb_kenn_bearb,
               vb_kenn,
               kd_segment_id,
               vt_segment_id,
               rd_segment_id,
               ads_user_id,
               cks_objekt_id,
               kkm_kampagne_id,
               cks_artikel_ausgegeben,
               cks_bearb_kenn,
               ve_kamp_anrtyp_id,
               kkm_kontakt_id,
               vorgang_id,
               import_status_flag,
               dwh_tarif_id
            )
            SELECT
               stichtag,
               vertrags_id,
               dwh_vertrag_id,
               vo_kenn,
               rahmenvertrag,
               dwh_tarifgr_id,
               aenderung_am,
               vvl_aendgrund_id,
               vvl_crd_alt,
               vvl_ersteperiode_alt,
               vvl_folgeperiode_alt,
               vertragsbindedatum_alt,
               vvl_crd_neu,
               vvl_ersteperiode_neu,
               vvl_folgeperiode_neu,
               vertragsbindedatum_neu,
               vertragsbeginn,
               ladedatum,
               vo_kenn_bearb,
               vb_kenn_bearb,
               vb_kenn,
               kd_segment_id,
               vt_segment_id,
               rd_segment_id,
               ads_user_id,
               cks_objekt_id,
               kkm_kampagne_id,
               cks_artikel_ausgegeben,
               cks_bearb_kenn,
               ve_kamp_anrtyp_id,
               kkm_kontakt_id,
               vorgang_id,
               import_status_flag,
               dwh_tarif_id
            FROM `{{ params.gcp_project_id }}.{{ params.isbert_source_dataset }}.dwh_ta_f_vvl_ereignisse` AS vvl
            WHERE
                (
                    vvl.vvl_aendgrund_id IN ( -3, 6, 7, 12, 13, 14, 15, 16, 17, 22, 80)
                    OR vvl.vvl_aendgrund_id BETWEEN 24 AND 60
                );
        """,
        use_legacy_sql=False,
        gcp_conn_id="gcp_connection_id",  # Ensure this connection is configured in Airflow
        # location="your-bigquery-location", # Specify BigQuery location, e.g., 'US', 'EU'
    )

    # End task, potentially for logging success or other post-processing
    # The original script captured record counts in a tmpFile.
    # You could add a PythonOperator here to query the `sof_ta_vvl_dwh` table
    # for a record count and push it to XComs if needed for downstream tasks.
    end_task = DummyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    start_task >> log_params_task >> execute_bigquery_sql >> end_task