-- BigQuery SQL for DW.BERT_AUSD_V_TA_P_VERTRAG
-- Replaces legacy Oracle SQL script d_ausd_v_ta_p_vertrag.sql

-- Determine processing date (v_datum)
-- In BigQuery, this can be done via a variable or a CTE.
-- For a script, we declare a variable.
DECLARE v_datum STRING;
SET v_datum = (SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
               FROM `project_id.isbert_schema.dwtk_meldungen` m
               WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE');

-- Truncate the target table
TRUNCATE TABLE `project_id.dataset_id.sof_ta_p_vertrag`;

-- Main INSERT INTO SELECT statement
INSERT INTO `project_id.dataset_id.sof_ta_p_vertrag`
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
        `project_id.dataset_id.sof_ta_vertrag_tmp` v
        LEFT JOIN `project_id.dataset_id.sof_ta_vertrag_tmp` pv
             ON v.twin_vertrag_id = pv.vertrag_id_carmen;

-- Truncate temporary tables
TRUNCATE TABLE `project_id.dataset_id.sof_ta_disc_zusgf`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_discount`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_barrier_zusgf`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_barrier`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_cntrct_crs`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_cntrct_templ`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_cntrct_valid`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_period`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_bp_ref`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_inv_assign`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_inv_def`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_acc_ref`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_notice`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_apn_ve`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_discount_rr`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_vvl_dwh`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_vvl_upgrade`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_cntrct_crs2`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_cntrct_crs3`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_inv_acc`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_vertrag_tmp`;
TRUNCATE TABLE `project_id.dataset_id.sof_ta_action_assoc`;