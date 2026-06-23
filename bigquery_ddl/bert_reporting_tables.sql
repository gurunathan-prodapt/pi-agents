--
-- Target BigQuery DDL for tables used by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
-- Inferred from source files k_ausd_austausch.ksh and d_ausd_austausch.sql
--

-- Dataset creation (if not exists)
CREATE SCHEMA IF NOT EXISTS `bert_reporting`;

-- job_audit_log table
CREATE TABLE IF NOT EXISTS `bert_reporting`.`job_audit_log`
(
    job_name STRING NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status STRING, -- 'SUCCESS', 'FAILED', 'RUNNING'
    message STRING,
    stichtag DATE,
    wiederanlauf_wert INT64,
    error_details STRING
);

-- Source tables (placeholders - replace with actual source table DDL or views)
-- The actual schemas of these source tables (sof$ta_p_...) are not explicitly defined
-- but inferred from the SELECT statements. These are examples.
CREATE TABLE IF NOT EXISTS `bert_reporting`.`sof_ta_p_rech_empf`
(
    kundenkonto STRING,
    rechdef_id STRING,
    dpps_kontonummer STRING,
    quelle STRING,
    akad_titel STRING,
    rechnungsempfaenger STRING,
    zusatz_1 STRING,
    zusatz_2 STRING,
    strasse STRING,
    plz STRING,
    wohnort STRING,
    bankname STRING,
    bank_kontonummer STRING,
    blz STRING,
    organisationseinheit STRING,
    land STRING,
    firma STRING,
    vorname STRING,
    nachname STRING,
    kun_nr_rech_empf STRING,
    mwst_kennzeichen STRING,
    iban STRING,
    bic STRING
);

CREATE TABLE IF NOT EXISTS `bert_reporting`.`sof_ta_p_vertrag`
(
    cntrct_ty INT64,
    vertrag_id_carmen STRING,
    partner_id_carmen STRING,
    rechdef_id_carmen STRING,
    kundenkonto STRING,
    rahmenvertrag_id STRING,
    rechnungslauf STRING,
    vo_kenn STRING,
    geplant_kuend DATE,
    eingang_kuend DATE,
    rv_action_id STRING,
    vertragsbeginn DATE,
    order_number STRING,
    vertragsstatus STRING,
    twincard STRING,
    dwh_tarifgr_text STRING,
    bindefrist DATE,
    vertragsbindung STRING,
    rechnungszahlart STRING,
    segment_id STRING,
    letztes_upgrade DATE,
    rechnungsmedium STRING,
    upgradeberechtigt STRING,
    apn STRING,
    vda STRING,
    upgradegrund STRING,
    sperrart STRING,
    sperrgrund STRING,
    stillegungszeitraum STRING,
    twin_vertrag_id STRING,
    cost_centre STRING,
    cost_centre_user STRING,
    mwst_kennzeichen STRING,
    rechn_inh_konfig_text STRING,
    commitment_reference_date DATE,
    cntrct_validity_id STRING,
    sv_id STRING
);

CREATE TABLE IF NOT EXISTS `bert_reporting`.`sof_ta_p_basisprod`
(
    cntrct_id STRING,
    tc_ms_stat STRING,
    ms1_ms_stat STRING,
    tnv_ms_stat STRING,
    tnv_msisdn STRING,
    tb_ms_stat STRING,
    tb_msisdn STRING,
    da_ms_stat STRING,
    da_msisdn STRING,
    vda_ms_stat STRING,
    vda_msisdn STRING,
    tk_ms_stat STRING,
    tk_msisdn STRING,
    evn INT64,
    tnv_dat_stat STRING,
    tnv_dat_msisdn STRING,
    tb_dat_stat STRING,
    tb_dat_msisdn STRING,
    tnv_fax_stat STRING,
    tnv_fax_msisdn STRING,
    tb_fax_stat STRING,
    tb_fax_msisdn STRING,
    data_option_rein STRING,
    voice_option_rein STRING,
    mix_option STRING,
    multi_option STRING,
    roaming_option STRING,
    sonstige_option STRING,
    apn STRING,
    tnv_icc_stat STRING,
    tnv_e_id STRING,
    tnv_card_type_name STRING,
    tb_icc_stat STRING,
    tb_e_id STRING,
    tb_card_type_name STRING,
    tc_icc_stat STRING,
    tc_e_id STRING,
    ms1_stat STRING,
    ms1_e_id STRING,
    ms1_card_type_name STRING,
    ms2_stat STRING,
    ms2_e_id STRING,
    ms2_card_type_name STRING,
    tnv_iccid STRING,
    tb_iccid STRING,
    tc_iccid STRING,
    ms1_iccid STRING,
    ms2_iccid STRING,
    tnv_hlr STRING,
    tb_hlr STRING,
    tc_hlr STRING,
    ms1_hlr STRING,
    ms2_hlr STRING,
    bcp_vertrag STRING,
    bcp_iccid STRING,
    bcp_hlr STRING,
    bcp_tn_tel STRING,
    ms3_stat STRING, ms3_iccid STRING, ms3_e_id STRING, ms3_card_type_name STRING, ms3_hlr STRING,
    ms4_stat STRING, ms4_iccid STRING, ms4_e_id STRING, ms4_card_type_name STRING, ms4_hlr STRING,
    ms5_stat STRING, ms5_iccid STRING, ms5_e_id STRING, ms5_card_type_name STRING, ms5_hlr STRING,
    ms6_stat STRING, ms6_iccid STRING, ms6_e_id STRING, ms6_card_type_name STRING, ms6_hlr STRING,
    ms7_stat STRING, ms7_iccid STRING, ms7_e_id STRING, ms7_card_type_name STRING, ms7_hlr STRING,
    ms8_stat STRING, ms8_iccid STRING, ms8_e_id STRING, ms8_card_type_name STRING, ms8_hlr STRING,
    ms9_stat STRING, ms9_iccid STRING, ms9_e_id STRING, ms9_card_type_name STRING, ms9_hlr STRING,
    ms10_stat STRING, ms10_iccid STRING, ms10_e_id STRING, ms10_card_type_name STRING, ms10_hlr STRING
);

CREATE TABLE IF NOT EXISTS `bert_reporting`.`sof_ta_p_gesch_part`
(
    tm_kundennummer STRING,
    cntrct_id STRING,
    firmenname STRING,
    akad_titel STRING,
    nachname STRING,
    vorname STRING,
    land STRING,
    plz STRING,
    wohnort STRING,
    strasse STRING,
    kunde_segment_id STRING,
    prem_segment_id INT64,
    organisationseinheit STRING,
    adresszusatz STRING,
    namenszusatz STRING,
    mwst_kennzeichen STRING
);

CREATE TABLE IF NOT EXISTS `bert_reporting`.`sof_ta_p_dn_nutzer`
(
    cntrct_id STRING,
    firmenname STRING,
    akad_titel STRING,
    nachname STRING,
    vorname STRING,
    land STRING,
    plz STRING,
    wohnort STRING,
    strasse STRING,
    organisationseinheit STRING,
    adresszusatz STRING,
    namenszusatz STRING,
    mwst_kennzeichen STRING
);

CREATE TABLE IF NOT EXISTS `bert_reporting`.`sof_ta_p_evn_empf`
(
    cntrct_id STRING,
    firmenname STRING,
    akad_titel STRING,
    nachname STRING,
    vorname STRING,
    land STRING,
    plz STRING,
    wohnort STRING,
    strasse STRING,
    organisationseinheit STRING,
    adresszusatz STRING,
    namenszusatz STRING,
    mwst_kennzeichen STRING
);

CREATE TABLE IF NOT EXISTS `bert_reporting`.`sof_ta_p_discount`
(
    contract_number STRING,
    rabatt_alle STRING
);

CREATE TABLE IF NOT EXISTS `bert_reporting`.`sof_ta_p_discount_rr`
(
    contract_number STRING,
    std_vertrag STRING,
    rabatt STRING,
    rabattierte_rech_pos STRING,
    rabatthoehe STRING,
    cntrct_template_id STRING,
    disc_invoice_item_id STRING
);

CREATE TABLE IF NOT EXISTS `bert_reporting`.`sof_ta_p_d1_vpn`
(
    vertrags_id STRING,
    vpn_id STRING
);


-- Target reporting tables (FOS tables)
CREATE TABLE IF NOT EXISTS `bert_reporting`.`rpt_ta_s_d1_rech_empf`
(
    dwh_konto_id STRING,
    rechdef_id_carmen STRING,
    konto_nr_dpps STRING,
    quelle STRING,
    anrede STRING,
    rechnungsempfaenger STRING,
    zusatz_1 STRING,
    zusatz_2 STRING,
    strasse STRING,
    plz STRING,
    wohnort STRING,
    bankname STRING,
    kontonummer STRING,
    blz STRING,
    organisationseinheit STRING,
    land STRING,
    firma STRING,
    vorname STRING,
    nachname STRING,
    kundennummer STRING,
    mwst_kennzeichen STRING,
    iban STRING,
    bic STRING
);

CREATE TABLE IF NOT EXISTS `bert_reporting`.`rpt_ta_s_d1_vertrag`
(
    kund_nr_dpps STRING,
    partner_id_carmen STRING,
    rechdef_id_carmen STRING,
    kundenkonto STRING,
    waehrung STRING,
    rahmenvertrag_id STRING,
    rechnungslauf STRING,
    vo_kenn STRING,
    geplant_kuend DATE,
    eingang_kuend DATE,
    rv_akz STRING,
    vertragsbeginn DATE,
    order_number STRING,
    vertragsstatus STRING,
    twincard STRING,
    msisdn STRING,
    dwh_tarifgr_text STRING,
    bindefrist DATE,
    vertragsbindung STRING,
    rechnungszahlart STRING,
    evn STRING,
    data96 STRING,
    fax STRING,
    firmenname STRING,
    akad_titel STRING,
    nachname STRING,
    vorname STRING,
    land STRING,
    plz STRING,
    wohnort STRING,
    strasse STRING,
    kunde_segment_id STRING,
    prem_segment_id STRING,
    rd_segment_id STRING,
    letztes_upgrade DATE,
    vertrag_id_carmen STRING,
    rechnungsmedium STRING,
    rueckgewinn_datum DATE,
    twin_msisdn STRING,
    organisationseinheit STRING,
    adresszusatz STRING,
    namenszusatz STRING,
    data_option_rein STRING,
    voice_option_rein STRING,
    mix_option STRING,
    multi_option STRING,
    roaming_option STRING,
    sonstige_option STRING,
    upgradeberechtigt STRING,
    apn STRING,
    vda STRING,
    upgradegrund STRING,
    e_id STRING,
    card_type_name STRING,
    link_e_id STRING,
    link_card_type_name STRING,
    ms2_e_id STRING,
    ms2_card_type_name STRING,
    iccid STRING,
    link_iccid STRING,
    ms2_iccid STRING,
    hlr STRING,
    link_hlr STRING,
    ms2_hlr STRING,
    sperrart STRING,
    sperrgrund STRING,
    stillegungszeitraum STRING,
    twin_vertrag_id STRING,
    cntrct_ty INT64,
    dn_firmenname STRING,
    dn_akad_titel STRING,
    dn_nachname STRING,
    dn_vorname STRING,
    dn_land STRING,
    dn_plz STRING,
    dn_wohnort STRING,
    dn_strasse STRING,
    dn_org_einheit STRING,
    dn_adresszusatz STRING,
    dn_namenszusatz STRING,
    ev_firmenname STRING,
    ev_akad_titel STRING,
    ev_nachname STRING,
    ev_vorname STRING,
    ev_land STRING,
    ev_plz STRING,
    ev_wohnort STRING,
    ev_strasse STRING,
    ev_org_einheit STRING,
    ev_adresszusatz STRING,
    ev_namenszusatz STRING,
    kostenstelle STRING,
    kostenstellennutzer STRING,
    bcp_vertrag STRING,
    bcp_iccid STRING,
    bcp_hlr STRING,
    gp_mwst_kennzeichen STRING,
    dn_mwst_kennzeichen STRING,
    ev_mwst_kennzeichen STRING,
    v_mwst_kennzeichen STRING,
    bcp_tn_tel STRING,
    rechn_inh_konfig_text STRING,
    commitment_reference_date DATE,
    cntrct_validity_id STRING,
    sv_id STRING,
    ms3_iccid STRING,
    ms3_e_id STRING,
    ms3_card_type_name STRING,
    ms3_hlr STRING,
    ms4_iccid STRING,
    ms4_e_id STRING,
    ms4_card_type_name STRING,
    ms4_hlr STRING,
    ms5_iccid STRING,
    ms5_e_id STRING,
    ms5_card_type_name STRING,
    ms5_hlr STRING,
    ms6_iccid STRING,
    ms6_e_id STRING,
    ms6_card_type_name STRING,
    ms6_hlr STRING,
    ms7_iccid STRING,
    ms7_e_id STRING,
    ms7_card_type_name STRING,
    ms7_hlr STRING,
    ms8_iccid STRING,
    ms8_e_id STRING,
    ms8_card_type_name STRING,
    ms8_hlr STRING,
    ms9_iccid STRING,
    ms9_e_id STRING,
    ms9_card_type_name STRING,
    ms9_hlr STRING,
    ms10_iccid STRING,
    ms10_e_id STRING,
    ms10_card_type_name STRING,
    ms10_hlr STRING
);

CREATE TABLE IF NOT EXISTS `bert_reporting`.`rpt_ta_s_d1_rech_kunde`
(
    kundenkonto STRING,
    kundennummer STRING
);

CREATE TABLE IF NOT EXISTS `bert_reporting`.`rpt_ta_s_d1_discount`
(
    contract_number STRING,
    rabatt_alle STRING
);

CREATE TABLE IF NOT EXISTS `bert_reporting`.`rpt_ta_s_d1_discount_rr`
(
    contract_number STRING,
    std_vertrag STRING,
    rabatt STRING,
    rabattierte_rech_pos STRING,
    rabatthoehe STRING,
    cntrct_template_id STRING,
    disc_invoice_item_id STRING
);

CREATE TABLE IF NOT EXISTS `bert_reporting`.`rpt_ta_s_d1_vpn`
(
    vertrag_id_carmen STRING,
    vpn_id STRING
);