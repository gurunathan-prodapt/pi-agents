-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
-- Legacy Source: d_ausd_austausch.sql

-- Placeholder for your project and dataset
-- REPLACE WITH YOUR ACTUAL PROJECT AND DATASET
CREATE SCHEMA IF NOT EXISTS `my_project.my_dataset` OPTIONS(description="BERT Data Warehouse - Migrated Tables");

-- DDL for Source Tables
-- These tables are assumed to be populated via an ingestion process from Oracle.
-- Data types are inferred; adjust as necessary based on actual Oracle schema.

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.sof_ta_p_rech_empf` (
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
)
OPTIONS(
    description="Migrated source table sof$ta_p_rech_empf"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.sof_ta_p_vertrag` (
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
    bindefrist STRING, -- Assuming this is a text description of a period
    vertragsbindung STRING,
    rechnungszahlart STRING,
    segment_id STRING,
    letztes_upgrade DATE,
    vertrag_id_carmen STRING,
    rechnungsmedium STRING,
    upgradeberechtigt STRING,
    apn STRING,
    vda STRING,
    upgradegrund STRING,
    sperrart STRING,
    sperrgrund STRING,
    stillegungszeitraum STRING,
    twin_vertrag_id STRING,
    cntrct_ty INT64,
    cost_centre STRING,
    cost_centre_user STRING,
    mwst_kennzeichen STRING,
    rechn_inh_konfig_text STRING,
    commitment_reference_date DATE,
    cntrct_validity_id STRING,
    sv_id STRING
)
OPTIONS(
    description="Migrated source table sof$ta_p_vertrag"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.sof_ta_p_basisprod` (
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
    tnv_icc_stat STRING,
    TNV_E_ID STRING,
    TB_E_ID STRING,
    TNV_CARD_TYPE_NAME STRING,
    TB_CARD_TYPE_NAME STRING,
    tc_icc_stat STRING,
    TC_E_ID STRING,
    ms1_stat STRING,
    MS1_E_ID STRING,
    MS2_E_ID STRING,
    MS2_CARD_TYPE_NAME STRING,
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
)
OPTIONS(
    description="Migrated source table sof$ta_p_basisprod"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.sof_ta_p_gesch_part` (
    cntrct_id STRING,
    tm_kundennummer STRING,
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
)
OPTIONS(
    description="Migrated source table sof$ta_p_gesch_part"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.sof_ta_p_dn_nutzer` (
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
)
OPTIONS(
    description="Migrated source table sof$ta_p_dn_nutzer"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.sof_ta_p_evn_empf` (
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
)
OPTIONS(
    description="Migrated source table sof$ta_p_evn_empf"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.sof_ta_p_discount` (
    contract_number STRING,
    rabatt_alle NUMERIC
)
OPTIONS(
    description="Migrated source table sof$ta_p_discount"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.sof_ta_p_discount_rr` (
    contract_number STRING,
    std_vertrag STRING,
    rabatt NUMERIC,
    rabattierte_rech_pos NUMERIC,
    rabatthoehe NUMERIC,
    cntrct_template_id STRING,
    disc_invoice_item_id STRING
)
OPTIONS(
    description="Migrated source table sof$ta_p_discount_rr"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.sof_ta_p_d1_vpn` (
    VERTRAGS_ID STRING,
    VPN_ID STRING
)
OPTIONS(
    description="Migrated source table sof$ta_p_d1_vpn"
);


-- DDL for Target Tables (final RPT$TA_S_D1_* tables)

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.rpt_ta_s_d1_rech_empf` (
    DWH_KONTO_ID STRING,
    RECHDEF_ID_CARMEN STRING,
    KONTO_NR_DPPS STRING,
    QUELLE STRING,
    ANREDE STRING,
    RECHNUNGSEMPFAENGER STRING,
    ZUSATZ_1 STRING,
    ZUSATZ_2 STRING,
    STRASSE STRING,
    PLZ STRING,
    WOHNORT STRING,
    BANKNAME STRING,
    KONTONUMMER STRING,
    BLZ STRING,
    ORGANISATIONSEINHEIT STRING,
    LAND STRING,
    FIRMA STRING,
    VORNAME STRING,
    NACHNAME STRING,
    KUNDENNUMMER STRING,
    MWST_KENNZEICHEN STRING,
    IBAN STRING,
    BIC STRING
)
OPTIONS(
    description="Target table rpt$ta_s_d1_rech_empf"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.rpt_ta_s_d1_vertrag` (
    KUND_NR_DPPS STRING,
    PARTNER_ID_CARMEN STRING,
    RECHDEF_ID_CARMEN STRING,
    KUNDENKONTO STRING,
    WAEHRUNG STRING,
    RAHMENVERTRAG_ID STRING,
    RECHNUNGSLAUF STRING,
    VO_KENN STRING,
    GEPLANT_KUEND DATE,
    EINGANG_KUEND DATE,
    RV_AKZ STRING,
    VERTRAGSBEGINN DATE,
    ORDER_NUMBER STRING,
    VERTRAGSSTATUS STRING,
    TWINCARD STRING,
    MSISDN STRING,
    DWH_TARIFGR_TEXT STRING,
    BINDEFRIST STRING,
    VERTRAGSBINDUNG STRING,
    RECHNUNGSZAHLART STRING,
    EVN STRING,
    DATA96 STRING,
    FAX STRING,
    FIRMENNAME STRING,
    AKAD_TITEL STRING,
    NACHNAME STRING,
    VORNAME STRING,
    LAND STRING,
    PLZ STRING,
    WOHNORT STRING,
    STRASSE STRING,
    KUNDE_SEGMENT_ID STRING,
    PREM_SEGMENT_ID STRING,
    RD_SEGMENT_ID STRING,
    LETZTES_UPGRADE DATE,
    VERTRAG_ID_CARMEN STRING,
    RECHNUNGSMEDIUM STRING,
    RUECKGEWINN_DATUM DATE,
    TWIN_MSISDN STRING,
    ORGANISATIONSEINHEIT STRING,
    ADRESSZUSATZ STRING,
    NAMENSZUSATZ STRING,
    DATA_OPTION_REIN STRING,
    VOICE_OPTION_REIN STRING,
    MIX_OPTION STRING,
    MULTI_OPTION STRING,
    ROAMING_OPTION STRING,
    SONSTIGE_OPTION STRING,
    UPGRADEBERECHTIGT STRING,
    APN STRING,
    VDA STRING,
    UPGRADEGRUND STRING,
    E_ID STRING,
    CARD_TYPE_NAME STRING,
    LINK_E_ID STRING,
    LINK_CARD_TYPE_NAME STRING,
    MS2_E_ID STRING,
    MS2_CARD_TYPE_NAME STRING,
    ICCID STRING,
    LINK_ICCID STRING,
    MS2_ICCID STRING,
    HLR STRING,
    LINK_HLR STRING,
    MS2_HLR STRING,
    SPERRART STRING,
    SPERRGRUND STRING,
    STILLEGUNGSZEITRAUM STRING,
    TWIN_VERTRAG_ID STRING,
    CNTRCT_TY INT64,
    DN_FIRMENNAME STRING,
    DN_AKAD_TITEL STRING,
    DN_NACHNAME STRING,
    DN_VORNAME STRING,
    DN_LAND STRING,
    DN_PLZ STRING,
    DN_WOHNORT STRING,
    DN_STRASSE STRING,
    DN_ORG_EINHEIT STRING,
    DN_ADRESSZUSATZ STRING,
    DN_NAMENSZUSATZ STRING,
    EV_FIRMENNAME STRING,
    EV_AKAD_TITEL STRING,
    EV_NACHNAME STRING,
    EV_VORNAME STRING,
    EV_LAND STRING,
    EV_PLZ STRING,
    EV_WOHNORT STRING,
    EV_STRASSE STRING,
    EV_ORG_EINHEIT STRING,
    EV_ADRESSZUSATZ STRING,
    EV_NAMENSZUSATZ STRING,
    KOSTENSTELLE STRING,
    KOSTENSTELLENNUTZER STRING,
    BCP_VERTRAG STRING,
    BCP_ICCID STRING,
    BCP_HLR STRING,
    GP_MWST_KENNZEICHEN STRING,
    DN_MWST_KENNZEICHEN STRING,
    EV_MWST_KENNZEICHEN STRING,
    V_MWST_KENNZEICHEN STRING,
    BCP_TN_TEL STRING,
    RECHN_INH_KONFIG_TEXT STRING,
    COMMITMENT_REFERENCE_DATE DATE,
    CNTRCT_VALIDITY_ID STRING,
    SV_ID STRING,
    MS3_ICCID STRING, MS3_E_ID STRING, MS3_CARD_TYPE_NAME STRING, MS3_HLR STRING,
    MS4_ICCID STRING, MS4_E_ID STRING, MS4_CARD_TYPE_NAME STRING, MS4_HLR STRING,
    MS5_ICCID STRING, MS5_E_ID STRING, MS5_CARD_TYPE_NAME STRING, MS5_HLR STRING,
    MS6_ICCID STRING, MS6_E_ID STRING, MS6_CARD_TYPE_NAME STRING, MS6_HLR STRING,
    MS7_ICCID STRING, MS7_E_ID STRING, MS7_CARD_TYPE_NAME STRING, MS7_HLR STRING,
    MS8_ICCID STRING, MS8_E_ID STRING, MS8_CARD_TYPE_NAME STRING, MS8_HLR STRING,
    MS9_ICCID STRING, MS9_E_ID STRING, MS9_CARD_TYPE_NAME STRING, MS9_HLR STRING,
    MS10_ICCID STRING, MS10_E_ID STRING, MS10_CARD_TYPE_NAME STRING, MS10_HLR STRING
)
OPTIONS(
    description="Target table rpt$ta_s_d1_vertrag"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.rpt_ta_s_d1_rech_kunde` (
    KUNDENKONTO STRING,
    KUNDENNUMMER STRING
)
OPTIONS(
    description="Target table rpt$ta_s_d1_rech_kunde"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.rpt_ta_s_d1_discount` (
    CONTRACT_NUMBER STRING,
    RABATT_ALLE NUMERIC
)
OPTIONS(
    description="Target table rpt$ta_s_d1_discount"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.rpt_ta_s_d1_discount_rr` (
    CONTRACT_NUMBER STRING,
    STD_VERTRAG STRING,
    RABATT NUMERIC,
    RABATTIERTE_RECH_POS NUMERIC,
    RABATTHOEHE NUMERIC,
    CNTRCT_TEMPLATE_ID STRING,
    DISC_INVOICE_ITEM_ID STRING
)
OPTIONS(
    description="Target table rpt$ta_s_d1_discount_rr"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.rpt_ta_s_d1_vpn` (
    VERTRAG_ID_CARMEN STRING,
    VPN_ID STRING
)
OPTIONS(
    description="Target table rpt$ta_s_d1_vpn"
);

-- DDL for Audit/Metadata Tables
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_audit_log` (
    job_nr STRING,
    job_kennung STRING,
    event_type STRING,
    event_ts TIMESTAMP,
    stichtag DATE,
    restart_value INT64,
    message STRING
)
OPTIONS(
    description="Audit log for ETL jobs"
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_sequence` (
    sequence_name STRING NOT NULL,
    current_value INT64 NOT NULL
)
OPTIONS(
    description="Table for generating unique job IDs"
);

-- Initialize job_sequence if it's empty
INSERT INTO `my_project.my_dataset.job_sequence` (sequence_name, current_value)
SELECT 'BERT_AUSTAUSCH', 0
WHERE NOT EXISTS (SELECT 1 FROM `my_project.my_dataset.job_sequence` WHERE sequence_name = 'BERT_AUSTAUSCH');