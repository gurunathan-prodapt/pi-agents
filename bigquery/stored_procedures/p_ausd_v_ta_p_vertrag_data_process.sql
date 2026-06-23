-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh
-- Description: Migrated data processing logic from d_ausd_v_ta_p_vertrag.sql to BigQuery.
CREATE OR REPLACE PROCEDURE `my_dataset.p_ausd_v_ta_p_vertrag_data_process`(
    p_job_kennung STRING,
    p_eintrags_nr STRING,
    OUT processed_records INT64
)
BEGIN
    -- Declare variables
    DECLARE v_datum STRING;
    DECLARE dynamic_sql STRING;

    -- Determine v_datum from a messages table, similar to original script's logic
    -- The original script uses `isbert_schema.dwtk_meldungen`.
    -- Assuming a BigQuery equivalent `my_dataset.dwtk_meldungen` exists.
    SET v_datum = (SELECT IFNULL(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
                   FROM `my_dataset.dwtk_meldungen` AS m
                   WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE');

    -- Truncate target table sof$ta_p_vertrag
    -- Original: isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_p_vertrag');
    -- Assuming `sof$ta_p_vertrag` translates to `my_dataset.sof_ta_p_vertrag`
    TRUNCATE TABLE `my_dataset.sof_ta_p_vertrag`;

    -- Main INSERT statement
    -- Original: INSERT INTO sof$ta_p_vertrag ... SELECT ... FROM sof$ta_vertrag_tmp v, sof$ta_vertrag_tmp pv WHERE v.twin_vertrag_id = pv.vertrag_id_carmen (+);
    -- The (+) syntax for LEFT JOIN is converted to standard SQL LEFT JOIN.
    -- Assuming `sof$ta_vertrag_tmp` translates to `my_dataset.sof_ta_vertrag_tmp`.
    INSERT INTO `my_dataset.sof_ta_p_vertrag` (
        vertrag_id_carmen,
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
        cntrct_validity_id
    )
    SELECT
        v.vertrag_id_carmen,
        v.partner_id_carmen,
        v.rechdef_id_carmen,
        v.kundenkonto,
        v.mwst_kennzeichen,
        v.rahmenvertrag_id AS rahmenvertrag_id,
        v.rechnungslauf,
        v.vo_kenn AS vo_kenn,
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
        `my_dataset.sof_ta_vertrag_tmp` AS v
    LEFT JOIN
        `my_dataset.sof_ta_vertrag_tmp` AS pv
    ON
        v.twin_vertrag_id = pv.vertrag_id_carmen;

    SET processed_records = (SELECT COUNT(*) FROM `my_dataset.sof_ta_p_vertrag`);

    -- Truncate temporary intermediate tables
    -- Original: isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_disc_zusgf DROP STORAGE'); etc.
    -- Assuming `sof$` tables translate to `my_dataset.sof_` tables.
    -- The `DROP STORAGE` and `REUSE STORAGE` clauses are Oracle-specific and removed.
    TRUNCATE TABLE `my_dataset.sof_ta_disc_zusgf`;
    TRUNCATE TABLE `my_dataset.sof_ta_discount`;
    TRUNCATE TABLE `my_dataset.sof_ta_barrier_zusgf`;
    TRUNCATE TABLE `my_dataset.sof_ta_barrier`;
    TRUNCATE TABLE `my_dataset.sof_ta_cntrct_crs`;
    TRUNCATE TABLE `my_dataset.sof_ta_cntrct_templ`;
    TRUNCATE TABLE `my_dataset.sof_ta_cntrct_valid`;
    TRUNCATE TABLE `my_dataset.sof_ta_period`;
    TRUNCATE TABLE `my_dataset.sof_ta_bp_ref`;
    TRUNCATE TABLE `my_dataset.sof_ta_inv_assign`;
    TRUNCATE TABLE `my_dataset.sof_ta_inv_def`;
    TRUNCATE TABLE `my_dataset.sof_ta_acc_ref`;
    TRUNCATE TABLE `my_dataset.sof_ta_notice`;
    -- TRUNCATE TABLE `my_dataset.sof_ta_barrier_hist`; -- Commented out in original
    TRUNCATE TABLE `my_dataset.sof_ta_apn_ve`;
    TRUNCATE TABLE `my_dataset.sof_ta_discount_rr`;
    TRUNCATE TABLE `my_dataset.sof_ta_vvl_dwh`;
    TRUNCATE TABLE `my_dataset.sof_ta_vvl_upgrade`;
    TRUNCATE TABLE `my_dataset.sof_ta_cntrct_crs2`;
    TRUNCATE TABLE `my_dataset.sof_ta_cntrct_crs3`;
    TRUNCATE TABLE `my_dataset.sof_ta_inv_acc`;
    TRUNCATE TABLE `my_dataset.sof_ta_vertrag_tmp`;
    TRUNCATE TABLE `my_dataset.sof_ta_action_assoc`;

END;