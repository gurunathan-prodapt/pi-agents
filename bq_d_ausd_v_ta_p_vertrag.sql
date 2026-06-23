-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh
--
-- Core SQL logic for populating the ta_p_vertrag table in BigQuery.
-- This script contains only the main INSERT statement.
-- Other operations like truncations are handled by the Airflow DAG.

INSERT INTO `sof_dwh.ta_p_vertrag` (
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
    `sof_dwh.ta_vertrag_tmp` v
LEFT JOIN
    `sof_dwh.ta_vertrag_tmp` pv
ON
    v.twin_vertrag_id = pv.vertrag_id_carmen;