# Migrated from KornShell script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh
# Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='k_ausd_v_ta_p_vertrag_dag',
    default_args=default_args,
    description='Orchestrates processing of contract data, migrated from k_ausd_v_ta_p_vertrag.ksh',
    schedule_interval=None, # Define your schedule here, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=['bigquery', 'contract_data'],
    params={
        'JobKennung': 'default_job_kennung',  # Default value for JobKennung
        'EintragsNr': 'default_eintragsnr',  # Default value for EintragsNr
    }
) as dag:
    execute_sql_script = BigQueryExecuteQueryOperator(
        task_id='execute_contract_data_processing',
        sql="""
            -- Stichtag ermitteln
            DECLARE v_datum STRING;
            SET v_datum = (
              SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
              FROM `isbert_schema.dwtk_meldungen` m
              WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
            );

            -- tabelle von vorherigem lauf leeren
            TRUNCATE TABLE `sof$ta_p_vertrag`;

            -- vertragstabelle sof$ta_p_vertrag (nachbearbeitung der twinbill vertraege)
            INSERT INTO `sof$ta_p_vertrag`
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
                   v.rahmenvertrag_id as rahmenvertrag_id,
                   v.rechnungslauf,
                   v.vo_kenn as vo_kenn,
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
                    `sof$ta_vertrag_tmp` v
              LEFT JOIN
                    `sof$ta_vertrag_tmp` pv
              ON
                    v.twin_vertrag_id = pv.vertrag_id_carmen;

            -- leeren der temporaeren zwischentabellen
            TRUNCATE TABLE `sof$ta_disc_zusgf`;
            TRUNCATE TABLE `sof$ta_discount`;
            TRUNCATE TABLE `sof$ta_barrier_zusgf`;
            TRUNCATE TABLE `sof$ta_barrier`;
            TRUNCATE TABLE `sof$ta_cntrct_crs`;
            TRUNCATE TABLE `sof$ta_cntrct_templ`;
            TRUNCATE TABLE `sof$ta_cntrct_valid`;
            TRUNCATE TABLE `sof$ta_period`;
            TRUNCATE TABLE `sof$ta_bp_ref`;
            TRUNCATE TABLE `sof$ta_inv_assign`;
            TRUNCATE TABLE `sof$ta_inv_def`;
            TRUNCATE TABLE `sof$ta_acc_ref`;
            TRUNCATE TABLE `sof$ta_notice`;
            TRUNCATE TABLE `sof$ta_apn_ve`;
            TRUNCATE TABLE `sof$ta_discount_rr`;
            TRUNCATE TABLE `sof$ta_vvl_dwh`;
            TRUNCATE TABLE `sof$ta_vvl_upgrade`;
            TRUNCATE TABLE `sof$ta_cntrct_crs2`;
            TRUNCATE TABLE `sof$ta_cntrct_crs3`;
            TRUNCATE TABLE `sof$ta_inv_acc`;
            TRUNCATE TABLE `sof$ta_vertrag_tmp`;
            TRUNCATE TABLE `sof$ta_action_assoc`;
        """,
        use_legacy_sql=False,
    )