"""
Airflow DAG for DW.BERT_AUSD_V_TA_P_VERTRAG
Replaces legacy UC4 job, KornShell wrappers, and Oracle SQL*Plus script.
"""

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.dummy import DummyOperator

PROJECT_ID = "project_id"
DATASET_ID = "dataset_id"
ISBERT_SCHEMA_DATASET_ID = "isbert_schema" # Assuming isbert_schema is a separate dataset

with DAG(
    dag_id="dw_bert_ausd_v_ta_p_vertrag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None, # Define your schedule here, e.g., "0 0 * * *" for daily
    tags=["bigquery", "data_warehouse", "contract_data"],
    doc_md=__doc__,
) as dag:
    start = DummyOperator(task_id="start")

    # Task 1: DDL for target and staging tables
    # In a production setup, DDLs might be managed separately or as a one-time setup.
    # For this migration, we include them in the DAG for completeness if they need to be dynamic.
    # For simplicity, we assume the schemas are stable and tables are pre-created.
    # If not, these tasks would create/update tables.

    create_sof_ta_p_vertrag_table = BigQueryExecuteQueryOperator(
        task_id="create_sof_ta_p_vertrag_table",
        sql="""
            CREATE TABLE IF NOT EXISTS `{}.{}.sof_ta_p_vertrag`
            (
                vertrag_id_carmen STRING,
                partner_id_carmen STRING,
                rechdef_id_carmen STRING,
                kundenkonto STRING,
                mwst_kennzeichen STRING,
                rahmenvertrag_id STRING,
                rechnungslauf STRING,
                vo_kenn STRING,
                geplant_kuend DATE,
                eingang_kuend DATE,
                vertragsbeginn DATE,
                vertragsstatus STRING,
                sperrart STRING,
                sperrgrund STRING,
                stillegungszeitraum STRING,
                twincard STRING,
                dwh_tarifgr_text STRING,
                bindefrist STRING,
                letztes_upgrade DATE,
                vertragsbindung STRING,
                vertragsbindungseinheit STRING,
                rechnungszahlart STRING,
                rechnungsmedium STRING,
                twin_vertrag_id STRING,
                upgradeberechtigt STRING,
                apn STRING,
                upgradegrund STRING,
                sv_id STRING,
                vda STRING,
                cost_centre STRING,
                cost_centre_user STRING,
                cntrct_ty STRING,
                segment_id STRING,
                rv_action_id STRING,
                rechn_inh_konfig_text STRING,
                order_number STRING,
                commitment_reference_date DATE,
                cntrct_validity_id STRING
            );
            """.format(PROJECT_ID, DATASET_ID),
        use_legacy_sql=False,
    )

    create_sof_ta_vertrag_tmp_table = BigQueryExecuteQueryOperator(
        task_id="create_sof_ta_vertrag_tmp_table",
        sql="""
            CREATE TABLE IF NOT EXISTS `{}.{}.sof_ta_vertrag_tmp`
            (
                vertrag_id_carmen STRING,
                partner_id_carmen STRING,
                rechdef_id_carmen STRING,
                kundenkonto STRING,
                mwst_kennzeichen STRING,
                rahmenvertrag_id STRING,
                rechnungslauf STRING,
                vo_kenn STRING,
                geplant_kuend DATE,
                eingang_kuend DATE,
                vertragsbeginn DATE,
                vertragsstatus STRING,
                sperrart STRING,
                sperrgrund STRING,
                stillegungszeitraum STRING,
                twincard STRING,
                dwh_tarifgr_text STRING,
                bindefrist STRING,
                letztes_upgrade DATE,
                vertragsbindung STRING,
                vertragsbindungseinheit STRING,
                rechnungszahlart STRING,
                rechnungsmedium STRING,
                twin_vertrag_id STRING,
                upgradeberechtigt STRING,
                apn STRING,
                upgradegrund STRING,
                sv_id STRING,
                vda STRING,
                cost_centre STRING,
                cost_centre_user STRING,
                cntrct_ty STRING,
                segment_id STRING,
                rv_action_id STRING,
                rechn_inh_konfig_text STRING,
                order_number STRING,
                commitment_reference_date DATE,
                cntrct_validity_id STRING
            );
            """.format(PROJECT_ID, DATASET_ID),
        use_legacy_sql=False,
    )

    create_isbert_dwtk_meldungen_table = BigQueryExecuteQueryOperator(
        task_id="create_isbert_dwtk_meldungen_table",
        sql="""
            CREATE SCHEMA IF NOT EXISTS `{}.{}`;
            CREATE TABLE IF NOT EXISTS `{}.{}.dwtk_meldungen`
            (
                timecreated TIMESTAMP,
                job_kennung STRING
            );
            """.format(PROJECT_ID, ISBERT_SCHEMA_DATASET_ID),
        use_legacy_sql=False,
    )

    # Placeholder for ingestion tasks.
    # These would typically be external systems or separate DAGs.
    # For this DAG, we assume sof_ta_vertrag_tmp and isbert_schema.dwtk_meldungen
    # are populated by an upstream process before this DAG runs.
    ingest_data_to_staging = DummyOperator(
        task_id="ingest_data_to_staging",
        doc_md="""
        Placeholder for tasks that ingest data into:
        - `{}.{}.sof_ta_vertrag_tmp`
        - `{}.{}.dwtk_meldungen`
        from source systems (e.g., CARMEN DB).
        This task does not perform any actual ingestion but represents the dependency.
        """.format(PROJECT_ID, DATASET_ID, PROJECT_ID, ISBERT_SCHEMA_DATASET_ID)
    )

    # Main transformation and load task
    transform_and_load_sof_ta_p_vertrag = BigQueryExecuteQueryOperator(
        task_id="transform_and_load_sof_ta_p_vertrag",
        sql="""
            -- Determine processing date (v_datum)
            DECLARE v_datum STRING;
            SET v_datum = (SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
                           FROM `{}.{}.dwtk_meldungen` m
                           WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE');

            -- Truncate the target table
            TRUNCATE TABLE `{}.{}.sof_ta_p_vertrag`;

            -- Main INSERT INTO SELECT statement
            INSERT INTO `{}.{}.sof_ta_p_vertrag`
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
                    `{}.{}.sof_ta_vertrag_tmp` v
                    LEFT JOIN `{}.{}.sof_ta_vertrag_tmp` pv
                         ON v.twin_vertrag_id = pv.vertrag_id_carmen;
            """.format(PROJECT_ID, ISBERT_SCHEMA_DATASET_ID, PROJECT_ID, DATASET_ID, PROJECT_ID, DATASET_ID, PROJECT_ID, DATASET_ID),
        use_legacy_sql=False,
    )

    # Task 3: Cleanup temporary tables
    cleanup_temp_tables = BigQueryExecuteQueryOperator(
        task_id="cleanup_temp_tables",
        sql="""
            TRUNCATE TABLE `{}.{}.sof_ta_disc_zusgf`;
            TRUNCATE TABLE `{}.{}.sof_ta_discount`;
            TRUNCATE TABLE `{}.{}.sof_ta_barrier_zusgf`;
            TRUNCATE TABLE `{}.{}.sof_ta_barrier`;
            TRUNCATE TABLE `{}.{}.sof_ta_cntrct_crs`;
            TRUNCATE TABLE `{}.{}.sof_ta_cntrct_templ`;
            TRUNCATE TABLE `{}.{}.sof_ta_cntrct_valid`;
            TRUNCATE TABLE `{}.{}.sof_ta_period`;
            TRUNCATE TABLE `{}.{}.sof_ta_bp_ref`;
            TRUNCATE TABLE `{}.{}.sof_ta_inv_assign`;
            TRUNCATE TABLE `{}.{}.sof_ta_inv_def`;
            TRUNCATE TABLE `{}.{}.sof_ta_acc_ref`;
            TRUNCATE TABLE `{}.{}.sof_ta_notice`;
            TRUNCATE TABLE `{}.{}.sof_ta_apn_ve`;
            TRUNCATE TABLE `{}.{}.sof_ta_discount_rr`;
            TRUNCATE TABLE `{}.{}.sof_ta_vvl_dwh`;
            TRUNCATE TABLE `{}.{}.sof_ta_vvl_upgrade`;
            TRUNCATE TABLE `{}.{}.sof_ta_cntrct_crs2`;
            TRUNCATE TABLE `{}.{}.sof_ta_cntrct_crs3`;
            TRUNCATE TABLE `{}.{}.sof_ta_inv_acc`;
            TRUNCATE TABLE `{}.{}.sof_ta_vertrag_tmp`;
            TRUNCATE TABLE `{}.{}.sof_ta_action_assoc`;
            """.format(
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
                PROJECT_ID, DATASET_ID,
            ),
        use_legacy_sql=False,
    )

    end = DummyOperator(task_id="end")

    # Define task dependencies
    start >> [
        create_sof_ta_p_vertrag_table,
        create_sof_ta_vertrag_tmp_table,
        create_isbert_dwtk_meldungen_table,
    ]
    # Ingest data needs to complete before transformation
    [
        create_sof_ta_p_vertrag_table,
        create_sof_ta_vertrag_tmp_table,
        create_isbert_dwtk_meldungen_table,
    ] >> ingest_data_to_staging

    ingest_data_to_staging >> transform_and_load_sof_ta_p_vertrag
    transform_and_load_sof_ta_p_vertrag >> cleanup_temp_tables
    cleanup_temp_tables >> end