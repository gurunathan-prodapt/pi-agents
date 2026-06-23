-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_austausch.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh

-- This BigQuery stored procedure translates the core data transformation logic
-- from the Oracle SQL script d_ausd_austausch.sql.

CREATE OR REPLACE PROCEDURE `project.reporting_dataset.D_AUSD_AUSTAUSCH_SP`(
    p_job_kennung STRING,
    p_eintrags_nr STRING,
    p_stichtag DATE,
    p_wiederanlauf_wert INT64,
    p_datum_heute DATE,
    p_datum_gestern DATE
)
BEGIN
    DECLARE v_records INT64 DEFAULT 0;

    -- Log start of transformation
    CALL `project.admin_dataset.log_message`(p_job_kennung, p_eintrags_nr, 'INFO', 'Starting D_AUSD_AUSTAUSCH_SP');

    -- ========================= Step01 ==================================
    -- Aktualisierung der Tabelle RPT$TA_S_D1_RECH_EMPF
    -- Original Oracle: insert into rpt$ta_s_d1_rech_empf_new, then rename/drop old
    -- BigQuery: Use MERGE or CREATE OR REPLACE TABLE for simplicity and atomicity if full refresh is intended.
    -- Given the original TRUNCATE and RENAME, a full refresh is implied.

    CALL `project.admin_dataset.log_message`(p_job_kennung, p_eintrags_nr, 'INFO', 'Updating rpt_ta_s_d1_rech_empf');

    CREATE OR REPLACE TABLE `project.reporting_dataset.rpt_ta_s_d1_rech_empf` AS
    SELECT
        kundenkonto AS DWH_KONTO_ID,
        rechdef_id AS RECHDEF_ID_CARMEN,
        dpps_kontonummer AS KONTO_NR_DPPS,
        quelle AS QUELLE,
        akad_titel AS ANREDE,
        rechnungsempfaenger AS RECHNUNGSEMPFAENGER,
        zusatz_1 AS ZUSATZ_1,
        zusatz_2 AS ZUSATZ_2,
        SUBSTR(strasse, 1, 45) AS STRASSE,
        plz AS PLZ,
        wohnort AS WOHNORT,
        bankname AS BANKNAME,
        bank_kontonummer AS KONTONUMMER,
        blz AS BLZ,
        organisationseinheit AS ORGANISATIONSEINHEIT,
        land AS LAND,
        SUBSTR(firma, 1, 40) AS FIRMA,
        vorname AS VORNAME,
        nachname AS NACHNAME,
        kun_nr_rech_empf AS KUNDENNUMMER,
        mwst_kennzeichen AS MWST_KENNZEICHEN,
        iban AS IBAN,
        bic AS BIC
    FROM
        `project.source_dataset.sof_ta_p_rech_empf`
    ;

    -- ========================= Step02 ==================================
    -- Aktualisierung der Tabelle RPT$TA_S_D1_VERTRAG
    -- Original Oracle: two large UNION ALL selects, then rename/drop old
    -- BigQuery: Use CREATE OR REPLACE TABLE

    CALL `project.admin_dataset.log_message`(p_job_kennung, p_eintrags_nr, 'INFO', 'Updating rpt_ta_s_d1_vertrag');

    CREATE OR REPLACE TABLE `project.reporting_dataset.rpt_ta_s_d1_vertrag` AS
    SELECT
        gp.tm_kundennummer AS KUND_NR_DPPS,
        v.partner_id_carmen AS PARTNER_ID_CARMEN,
        v.rechdef_id_carmen AS RECHDEF_ID_CARMEN,
        v.kundenkonto AS KUNDENKONTO,
        'EUR' AS WAEHRUNG,
        SUBSTR(v.rahmenvertrag_id, 1, 10) AS RAHMENVERTRAG_ID,
        v.rechnungslauf AS RECHNUNGSLAUF,
        CAST(v.vo_kenn AS STRING) AS VO_KENN, -- to_char
        v.geplant_kuend AS GEPLANT_KUEND,
        v.eingang_kuend AS EINGANG_KUEND,
        v.rv_action_id AS RV_AKZ,
        v.vertragsbeginn AS VERTRAGSBEGINN,
        v.order_number AS ORDER_NUMBER,
        v.vertragsstatus AS VERTRAGSSTATUS,
        -- Twincard
        CASE
            WHEN v.twincard = 'TB' THEN v.twincard
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_ms_stat = 'A' THEN 'TC'
                    WHEN bp.ms1_ms_stat = 'A' THEN 'MS'
                    ELSE ' '
                END
            WHEN bp.tc_ms_stat IS NOT NULL THEN 'TC'
            ELSE ' '
        END AS TWINCARD,
        -- MSISDN
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_ms_stat = 'A' THEN bp.tnv_msisdn
                    ELSE
                        CASE
                            WHEN bp.tb_ms_stat = 'A' THEN bp.tb_msisdn
                            ELSE
                                CASE
                                    WHEN bp.da_ms_stat = 'A' THEN bp.da_msisdn
                                    ELSE
                                        CASE
                                            WHEN bp.vda_ms_stat = 'A' THEN bp.vda_msisdn
                                            ELSE
                                                CASE
                                                    WHEN bp.tk_ms_stat = 'A' THEN bp.tk_msisdn
                                                    ELSE ' '
                                                END
                                        END
                                END
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_ms_stat IS NOT NULL THEN bp.tnv_msisdn
                    ELSE
                        CASE
                            WHEN bp.tb_ms_stat IS NOT NULL THEN bp.tb_msisdn
                            ELSE
                                CASE
                                    WHEN bp.da_ms_stat IS NOT NULL THEN bp.da_msisdn
                                    ELSE
                                        CASE
                                            WHEN bp.vda_ms_stat IS NOT NULL THEN bp.vda_msisdn
                                            ELSE
                                                CASE
                                                    WHEN bp.tk_ms_stat IS NOT NULL THEN bp.tk_msisdn
                                                    ELSE ' '
                                                END
                                        END
                                END
                        END
                END
        END AS MSISDN,
        v.dwh_tarifgr_text AS DWH_TARIFGR_TEXT,
        v.bindefrist AS BINDEFRIST,
        v.vertragsbindung AS VERTRAGSBINDUNG,
        v.rechnungszahlart AS RECHNUNGSZAHLART,
        -- EVN
        CASE bp.evn
            WHEN 1 THEN 'Standard'
            WHEN 2 THEN 'Komfort'
            WHEN 3 THEN 'Komfort-Plus'
            WHEN 4 THEN 'Standard-Plus'
            WHEN 10 THEN 'separater EVN (Standard)'
            WHEN 20 THEN 'separater EVN (Komfort)'
            WHEN 30 THEN 'separater EVN (Komfort-Plus)'
            WHEN 11 THEN 'Standard / separater EVN (Standard)'
            WHEN 21 THEN 'Standard / separater EVN (Komfort)'
            WHEN 31 THEN 'Standard / separater EVN (Komfort-Plus)'
            WHEN 12 THEN 'Komfort / separater EVN (Standard)'
            WHEN 22 THEN 'Komfort / separater EVN (Komfort)'
            WHEN 32 THEN 'Komfort / separater EVN (Komfort-Plus)'
            WHEN 13 THEN 'Komfort-Plus / separater EVN (Standard)'
            WHEN 23 THEN 'Komfort-Plus / separater EVN (Komfort)'
            WHEN 33 THEN 'Komfort-Plus / separater EVN (Komfort-Plus)'
            WHEN 14 THEN 'Standard-Plus / separater EVN (Standard)'
            WHEN 24 THEN 'Standard-Plus / separater EVN (Komfort)'
            WHEN 34 THEN 'Standard-Plus / separater EVN (Komfort-Plus)'
            ELSE 'nein'
        END AS EVN,
        -- DATA96
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_dat_stat = 'A' THEN SUBSTR(bp.tnv_dat_msisdn, 1, 13)
                    ELSE
                        CASE
                            WHEN bp.tb_dat_stat = 'A' THEN SUBSTR(bp.tb_dat_msisdn, 1, 13)
                            ELSE
                                CASE
                                    WHEN bp.tnv_ms_stat = 'A' THEN SUBSTR(bp.tnv_msisdn, 1, 13)
                                    ELSE
                                        CASE
                                            WHEN bp.tb_ms_stat = 'A' THEN SUBSTR(bp.tb_msisdn, 1, 13)
                                            ELSE ' '
                                        END
                                END
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_dat_stat IS NOT NULL THEN SUBSTR(bp.tnv_dat_msisdn, 1, 13)
                    ELSE
                        CASE
                            WHEN bp.tb_dat_stat IS NOT NULL THEN SUBSTR(bp.tb_dat_msisdn, 1, 13)
                            ELSE
                                CASE
                                    WHEN bp.tnv_ms_stat IS NOT NULL THEN SUBSTR(bp.tnv_msisdn, 1, 13)
                                    ELSE
                                        CASE
                                            WHEN bp.tb_ms_stat IS NOT NULL THEN SUBSTR(bp.tb_msisdn, 1, 13)
                                            ELSE ' '
                                        END
                                END
                        END
                END
        END AS DATA96,
        -- FAX
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_fax_stat = 'A' THEN SUBSTR(bp.tnv_fax_msisdn, 1, 13)
                    ELSE
                        CASE
                            WHEN bp.tb_fax_stat = 'A' THEN SUBSTR(bp.tb_fax_msisdn, 1, 13)
                            ELSE
                                CASE
                                    WHEN bp.tnv_ms_stat = 'A' THEN SUBSTR(bp.tnv_msisdn, 1, 13)
                                    ELSE
                                        CASE
                                            WHEN bp.tb_ms_stat = 'A' THEN SUBSTR(bp.tb_msisdn, 1, 13)
                                            ELSE ' '
                                        END
                                END
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_fax_stat IS NOT NULL THEN SUBSTR(bp.tnv_fax_msisdn, 1, 13)
                    ELSE
                        CASE
                            WHEN bp.tb_fax_stat IS NOT NULL THEN SUBSTR(bp.tb_fax_msisdn, 1, 13)
                            ELSE
                                CASE
                                    WHEN bp.tnv_ms_stat IS NOT NULL THEN SUBSTR(bp.tnv_msisdn, 1, 13)
                                    ELSE
                                        CASE
                                            WHEN bp.tb_ms_stat IS NOT NULL THEN SUBSTR(bp.tb_msisdn, 1, 13)
                                            ELSE ' '
                                        END
                                END
                        END
                END
        END AS FAX,
        gp.firmenname AS FIRMENNAME,
        gp.akad_titel AS AKAD_TITEL,
        gp.nachname AS NACHNAME,
        gp.vorname AS VORNAME,
        gp.land AS LAND,
        gp.plz AS PLZ,
        gp.wohnort AS WOHNORT,
        gp.strasse AS STRASSE,
        SUBSTR(gp.kunde_segment_id, 1, 2) AS KUNDE_SEGMENT_ID,
        CASE WHEN gp.prem_segment_id = 1 THEN 'ja' ELSE 'nein' END AS PREM_SEGMENT_ID,
        v.segment_id AS RD_SEGMENT_ID,
        v.letztes_upgrade AS LETZTES_UPGRADE,
        v.vertrag_id_carmen AS VERTRAG_ID_CARMEN,
        v.rechnungsmedium AS RECHNUNGSMEDIUM,
        PARSE_DATE('%d.%m.%Y', '11.11.1111') AS RUECKGEWINN_DATUM, -- to_date
        -- TWIN_MSISDN
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_ms_stat = 'A' THEN bp.tc_msisdn
                    WHEN bp.ms1_stat = 'A' THEN bp.tnv_msisdn -- MS: Rufnummer Masterkarte
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_ms_stat = 'A' THEN bpt.tb_msisdn
                            WHEN bpt.tnv_ms_stat = 'A' THEN bpt.tnv_msisdn
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_ms_stat IS NOT NULL THEN bp.tc_msisdn
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_ms_stat IS NOT NULL THEN bpt.tb_msisdn
                    WHEN bpt.tnv_ms_stat IS NOT NULL THEN bpt.tnv_msisdn
                    ELSE ' '
                END
            ELSE ' '
        END AS TWIN_MSISDN,
        gp.organisationseinheit AS ORGANISATIONSEINHEIT,
        gp.adresszusatz AS ADRESSZUSATZ,
        gp.namenszusatz AS NAMENSZUSATZ,
        IFNULL(bp.data_option_rein, bpt.data_option_rein) AS DATA_OPTION_REIN,
        IFNULL(bp.voice_option_rein, bpt.voice_option_rein) AS VOICE_OPTION_REIN,
        IFNULL(bp.mix_option, bpt.mix_option) AS MIX_OPTION,
        IFNULL(bp.multi_option, bpt.multi_option) AS MULTI_OPTION,
        IFNULL(bp.roaming_option, bpt.roaming_option) AS ROAMING_OPTION,
        IFNULL(bp.sonstige_option, bpt.sonstige_option) AS SONSTIGE_OPTION,
        v.upgradeberechtigt AS UPGRADEBERECHTIGT,
        IFNULL(bp.apn, v.apn) AS APN,
        v.vda AS VDA,
        v.upgradegrund AS UPGRADEGRUND,
        -- E_ID
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_icc_stat = 'A' THEN bp.TNV_E_ID
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat = 'A' THEN bp.TB_E_ID
                            ELSE ' '
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_icc_stat IS NOT NULL THEN bp.TNV_E_ID
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat IS NOT NULL THEN bp.TB_E_ID
                            ELSE ' '
                        END
                END
        END AS E_ID,
        -- CARD_TYPE_NAME
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_icc_stat = 'A' THEN bp.TNV_CARD_TYPE_NAME
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat = 'A' THEN bp.TB_CARD_TYPE_NAME
                            ELSE ' '
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_icc_stat IS NOT NULL THEN bp.TNV_CARD_TYPE_NAME
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat IS NOT NULL THEN bp.TB_CARD_TYPE_NAME
                            ELSE ' '
                        END
                END
        END AS CARD_TYPE_NAME,
        -- LINK_E_ID
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_icc_stat = 'A' THEN bp.TC_E_ID
                    WHEN bp.ms1_stat = 'A' THEN bp.MS1_E_ID
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_icc_stat = 'A' THEN bpt.TB_E_ID
                            WHEN bpt.tnv_icc_stat = 'A' THEN bpt.TNV_E_ID
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_icc_stat IS NOT NULL THEN bp.TC_E_ID
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_icc_stat IS NOT NULL THEN bpt.TB_E_ID
                    WHEN bpt.tnv_icc_stat IS NOT NULL THEN bpt.TNV_E_ID
                    ELSE ' '
                END
            ELSE ' '
        END AS LINK_E_ID,
        -- LINK_CARD_TYPE_NAME
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_icc_stat = 'A' THEN bp.TC_CARD_TYPE_NAME
                    WHEN bp.ms1_stat = 'A' THEN bp.MS1_CARD_TYPE_NAME
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_icc_stat = 'A' THEN bpt.TB_CARD_TYPE_NAME
                            WHEN bpt.tnv_icc_stat = 'A' THEN bpt.TNV_CARD_TYPE_NAME
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_icc_stat IS NOT NULL THEN bp.TC_CARD_TYPE_NAME
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_icc_stat IS NOT NULL THEN bpt.TB_CARD_TYPE_NAME
                    WHEN bpt.tnv_icc_stat IS NOT NULL THEN bpt.TNV_CARD_TYPE_NAME
                    ELSE ' '
                END
            ELSE ' '
        END AS LINK_CARD_TYPE_NAME,
        -- MS2_E_ID
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.MS2_E_ID ELSE ' ' END AS MS2_E_ID,
        -- MS2_CARD_TYPE_NAME
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.MS2_CARD_TYPE_NAME ELSE ' ' END AS MS2_CARD_TYPE_NAME,
        -- ICCID
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_icc_stat = 'A' THEN bp.tnv_iccid
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat = 'A' THEN bp.tb_iccid
                            ELSE ' '
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_icc_stat IS NOT NULL THEN bp.tnv_iccid
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat IS NOT NULL THEN bp.tb_iccid
                            ELSE ' '
                        END
                END
        END AS ICCID,
        -- LINK_ICCID
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_icc_stat = 'A' THEN bp.tc_iccid
                    WHEN bp.ms1_stat = 'A' THEN bp.ms1_iccid
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_icc_stat = 'A' THEN bpt.tb_iccid
                            WHEN bpt.tnv_icc_stat = 'A' THEN bpt.tnv_iccid
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_icc_stat IS NOT NULL THEN bp.tc_iccid
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_icc_stat IS NOT NULL THEN bpt.tb_iccid
                    WHEN bpt.tnv_icc_stat IS NOT NULL THEN bpt.tnv_iccid
                    ELSE ' '
                END
            ELSE ' '
        END AS LINK_ICCID,
        -- MS2_ICCID
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.ms2_iccid ELSE ' ' END AS MS2_ICCID,
        -- HLR
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_icc_stat = 'A' THEN bp.tnv_hlr
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat = 'A' THEN bp.tb_hlr
                            ELSE ' '
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_icc_stat IS NOT NULL THEN bp.tnv_hlr
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat IS NOT NULL THEN bp.tb_hlr
                            ELSE ' '
                        END
                END
        END AS HLR,
        -- LINK_HLR
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_icc_stat = 'A' THEN bp.tc_hlr
                    WHEN bp.ms1_stat = 'A' THEN bp.ms1_hlr
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_icc_stat = 'A' THEN bpt.tb_hlr
                            WHEN bpt.tnv_icc_stat = 'A' THEN bpt.tnv_hlr
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_icc_stat IS NOT NULL THEN bp.tc_hlr
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_icc_stat IS NOT NULL THEN bpt.tb_hlr
                    WHEN bpt.tnv_icc_stat IS NOT NULL THEN bpt.tnv_hlr
                    ELSE ' '
                END
            ELSE ' '
        END AS LINK_HLR,
        -- MS2_HLR
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.ms2_hlr ELSE ' ' END AS MS2_HLR,
        v.sperrart AS SPERRART,
        v.sperrgrund AS SPERRGRUND,
        v.stillegungszeitraum AS STILLEGUNGSZEITRAUM,
        v.twin_vertrag_id AS TWIN_VERTRAG_ID,
        v.cntrct_ty AS CNTRCT_TY,
        dn.firmenname AS DN_FIRMENNAME,
        dn.akad_titel AS DN_AKAD_TITEL,
        dn.nachname AS DN_NACHNAME,
        dn.vorname AS DN_VORNAME,
        dn.land AS DN_LAND,
        dn.plz AS DN_PLZ,
        dn.wohnort AS DN_WOHNORT,
        dn.strasse AS DN_STRASSE,
        dn.organisationseinheit AS DN_ORG_EINHEIT,
        dn.adresszusatz AS DN_ADRESSZUSATZ,
        dn.namenszusatz AS DN_NAMENSZUSATZ,
        ev.firmenname AS EV_FIRMENNAME,
        ev.akad_titel AS EV_AKAD_TITEL,
        ev.nachname AS EV_NACHNAME,
        ev.vorname AS EV_VORNAME,
        ev.land AS EV_LAND,
        ev.plz AS EV_PLZ,
        ev.wohnort AS EV_WOHNORT,
        ev.strasse AS EV_STRASSE,
        ev.organisationseinheit AS EV_ORG_EINHEIT,
        ev.adresszusatz AS EV_ADRESSZUSATZ,
        ev.namenszusatz AS EV_NAMENSZUSATZ,
        v.cost_centre AS KOSTENSTELLE,
        v.cost_centre_user AS KOSTENSTELLENNUTZER,
        IFNULL(bp.bcp_vertrag, bpt.bcp_vertrag) AS BCP_VERTRAG,
        IFNULL(bp.bcp_iccid, bpt.bcp_iccid) AS BCP_ICCID,
        IFNULL(bp.bcp_hlr, bpt.bcp_hlr) AS BCP_HLR,
        gp.mwst_kennzeichen AS GP_MWST_KENNZEICHEN,
        dn.mwst_kennzeichen AS DN_MWST_KENNZEICHEN,
        ev.mwst_kennzeichen AS EV_MWST_KENNZEICHEN,
        v.mwst_kennzeichen AS V_MWST_KENNZEICHEN,
        IFNULL(bp.bcp_tn_tel, bpt.bcp_tn_tel) AS BCP_TN_TEL, -- decode(bp.bcp_vertrag,NULL,bpt.bcp_tn_tel,bp.bcp_tn_tel)
        RECHN_INH_KONFIG_TEXT, -- This column comes from v, assuming direct mapping
        v.commitment_reference_date AS COMMITMENT_REFERENCE_DATE,
        v.cntrct_validity_id AS CNTRCT_VALIDITY_ID,
        v.sv_id AS SV_ID,
        -- MultiSim 3 Plus START
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_iccid ELSE ' ' END AS MS3_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_e_id ELSE ' ' END AS MS3_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_card_type_name ELSE ' ' END AS MS3_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_hlr ELSE ' ' END AS MS3_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_iccid ELSE ' ' END AS MS4_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_e_id ELSE ' ' END AS MS4_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_card_type_name ELSE ' ' END AS MS4_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_hlr ELSE ' ' END AS MS4_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_iccid ELSE ' ' END AS MS5_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_e_id ELSE ' ' END AS MS5_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_card_type_name ELSE ' ' END AS MS5_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_hlr ELSE ' ' END AS MS5_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_iccid ELSE ' ' END AS MS6_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_e_id ELSE ' ' END AS MS6_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_card_type_name ELSE ' ' END AS MS6_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_hlr ELSE ' ' END AS MS6_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_iccid ELSE ' ' END AS MS7_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_e_id ELSE ' ' END AS MS7_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_card_type_name ELSE ' ' END AS MS7_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_hlr ELSE ' ' END AS MS7_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_iccid ELSE ' ' END AS MS8_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_e_id ELSE ' ' END AS MS8_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_card_type_name ELSE ' ' END AS MS8_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_hlr ELSE ' ' END AS MS8_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_iccid ELSE ' ' END AS MS9_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_e_id ELSE ' ' END AS MS9_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_card_type_name ELSE ' ' END AS MS9_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_hlr ELSE ' ' END AS MS9_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_iccid ELSE ' ' END AS MS10_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_e_id ELSE ' ' END AS MS10_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_card_type_name ELSE ' ' END AS MS10_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_hlr ELSE ' ' END AS MS10_HLR
    FROM
        `project.source_dataset.sof_ta_p_vertrag` AS v
    JOIN
        `project.source_dataset.sof_ta_p_gesch_part` AS gp
        ON v.vertrag_id_carmen = gp.cntrct_id
    LEFT JOIN
        `project.source_dataset.sof_ta_p_basisprod` AS bp
        ON v.vertrag_id_carmen = bp.cntrct_id
    LEFT JOIN
        `project.source_dataset.sof_ta_p_basisprod` AS bpt
        ON v.twin_vertrag_id = bpt.cntrct_id
    LEFT JOIN
        `project.source_dataset.sof_ta_p_evn_empf` AS ev
        ON v.vertrag_id_carmen = ev.cntrct_id
    LEFT JOIN
        `project.source_dataset.sof_ta_p_dn_nutzer` AS dn
        ON v.vertrag_id_carmen = dn.cntrct_id
    WHERE
        v.cntrct_ty <> 11 -- keine RV-Zusatzvertraege
        AND v.cntrct_ty <> 20 -- keine Mobilfunkzusatzvertraege

    UNION ALL

    SELECT
        gp.tm_kundennummer AS KUND_NR_DPPS,
        v.partner_id_carmen AS PARTNER_ID_CARMEN,
        v.rechdef_id_carmen AS RECHDEF_ID_CARMEN,
        v.kundenkonto AS KUNDENKONTO,
        'EUR' AS WAEHRUNG,
        SUBSTR(v.rahmenvertrag_id, 1, 10) AS RAHMENVERTRAG_ID,
        v.rechnungslauf AS RECHNUNGSLAUF,
        CAST(v.vo_kenn AS STRING) AS VO_KENN, -- to_char
        v.geplant_kuend AS GEPLANT_KUEND,
        v.eingang_kuend AS EINGANG_KUEND,
        v.rv_action_id AS RV_AKZ,
        v.vertragsbeginn AS VERTRAGSBEGINN,
        v.order_number AS ORDER_NUMBER,
        v.vertragsstatus AS VERTRAGSSTATUS,
        -- Twincard
        CASE
            WHEN v.twincard = 'TB' THEN v.twincard
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_ms_stat = 'A' THEN 'TC'
                    WHEN bp.ms1_ms_stat = 'A' THEN 'MS'
                    ELSE ' '
                END
            WHEN bp.tc_ms_stat IS NOT NULL THEN 'TC'
            ELSE ' '
        END AS TWINCARD,
        -- MSISDN
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_ms_stat = 'A' THEN bp.tnv_msisdn
                    ELSE
                        CASE
                            WHEN bp.tb_ms_stat = 'A' THEN bp.tb_msisdn
                            ELSE
                                CASE
                                    WHEN bp.da_ms_stat = 'A' THEN bp.da_msisdn
                                    ELSE
                                        CASE
                                            WHEN bp.vda_ms_stat = 'A' THEN bp.vda_msisdn
                                            ELSE
                                                CASE
                                                    WHEN bp.tk_ms_stat = 'A' THEN bp.tk_msisdn
                                                    ELSE ' '
                                                END
                                        END
                                END
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_ms_stat IS NOT NULL THEN bp.tnv_msisdn
                    ELSE
                        CASE
                            WHEN bp.tb_ms_stat IS NOT NULL THEN bp.tb_msisdn
                            ELSE
                                CASE
                                    WHEN bp.da_ms_stat IS NOT NULL THEN bp.da_msisdn
                                    ELSE
                                        CASE
                                            WHEN bp.vda_ms_stat IS NOT NULL THEN bp.vda_msisdn
                                            ELSE
                                                CASE
                                                    WHEN bp.tk_ms_stat IS NOT NULL THEN bp.tk_msisdn
                                                    ELSE ' '
                                                END
                                        END
                                END
                        END
                END
        END AS MSISDN,
        v.dwh_tarifgr_text AS DWH_TARIFGR_TEXT,
        v.bindefrist AS BINDEFRIST,
        v.vertragsbindung AS VERTRAGSBINDUNG,
        v.rechnungszahlart AS RECHNUNGSZAHLART,
        -- EVN
        CASE bp.evn
            WHEN 1 THEN 'Standard'
            WHEN 2 THEN 'Komfort'
            WHEN 3 THEN 'Komfort-Plus'
            WHEN 4 THEN 'Standard-Plus'
            WHEN 10 THEN 'separater EVN (Standard)'
            WHEN 20 THEN 'separater EVN (Komfort)'
            WHEN 30 THEN 'separater EVN (Komfort-Plus)'
            WHEN 11 THEN 'Standard / separater EVN (Standard)'
            WHEN 21 THEN 'Standard / separater EVN (Komfort)'
            WHEN 31 THEN 'Standard / separater EVN (Komfort-Plus)'
            WHEN 12 THEN 'Komfort / separater EVN (Standard)'
            WHEN 22 THEN 'Komfort / separater EVN (Komfort)'
            WHEN 32 THEN 'Komfort / separater EVN (Komfort-Plus)'
            WHEN 13 THEN 'Komfort-Plus / separater EVN (Standard)'
            WHEN 23 THEN 'Komfort-Plus / separater EVN (Komfort)'
            WHEN 33 THEN 'Komfort-Plus / separater EVN (Komfort-Plus)'
            WHEN 14 THEN 'Standard-Plus / separater EVN (Standard)'
            WHEN 24 THEN 'Standard-Plus / separater EVN (Komfort)'
            WHEN 34 THEN 'Standard-Plus / separater EVN (Komfort-Plus)'
            ELSE 'nein'
        END AS EVN,
        -- DATA96
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_dat_stat = 'A' THEN SUBSTR(bp.tnv_dat_msisdn, 1, 13)
                    ELSE
                        CASE
                            WHEN bp.tb_dat_stat = 'A' THEN SUBSTR(bp.tb_dat_msisdn, 1, 13)
                            ELSE
                                CASE
                                    WHEN bp.tnv_ms_stat = 'A' THEN SUBSTR(bp.tnv_msisdn, 1, 13)
                                    ELSE
                                        CASE
                                            WHEN bp.tb_ms_stat = 'A' THEN SUBSTR(bp.tb_msisdn, 1, 13)
                                            ELSE ' '
                                        END
                                END
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_dat_stat IS NOT NULL THEN SUBSTR(bp.tnv_dat_msisdn, 1, 13)
                    ELSE
                        CASE
                            WHEN bp.tb_dat_stat IS NOT NULL THEN SUBSTR(bp.tb_dat_msisdn, 1, 13)
                            ELSE
                                CASE
                                    WHEN bp.tnv_ms_stat IS NOT NULL THEN SUBSTR(bp.tnv_msisdn, 1, 13)
                                    ELSE
                                        CASE
                                            WHEN bp.tb_ms_stat IS NOT NULL THEN SUBSTR(bp.tb_msisdn, 1, 13)
                                            ELSE ' '
                                        END
                                END
                        END
                END
        END AS DATA96,
        -- FAX
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_fax_stat = 'A' THEN SUBSTR(bp.tnv_fax_msisdn, 1, 13)
                    ELSE
                        CASE
                            WHEN bp.tb_fax_stat = 'A' THEN SUBSTR(bp.tb_fax_msisdn, 1, 13)
                            ELSE
                                CASE
                                    WHEN bp.tnv_ms_stat = 'A' THEN SUBSTR(bp.tnv_msisdn, 1, 13)
                                    ELSE
                                        CASE
                                            WHEN bp.tb_ms_stat = 'A' THEN SUBSTR(bp.tb_msisdn, 1, 13)
                                            ELSE ' '
                                        END
                                END
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_fax_stat IS NOT NULL THEN SUBSTR(bp.tnv_fax_msisdn, 1, 13)
                    ELSE
                        CASE
                            WHEN bp.tb_fax_stat IS NOT NULL THEN SUBSTR(bp.tb_fax_msisdn, 1, 13)
                            ELSE
                                CASE
                                    WHEN bp.tnv_ms_stat IS NOT NULL THEN SUBSTR(bp.tnv_msisdn, 1, 13)
                                    ELSE
                                        CASE
                                            WHEN bp.tb_ms_stat IS NOT NULL THEN SUBSTR(bp.tb_msisdn, 1, 13)
                                            ELSE ' '
                                        END
                                END
                        END
                END
        END AS FAX,
        gp.firmenname AS FIRMENNAME,
        gp.akad_titel AS AKAD_TITEL,
        gp.nachname AS NACHNAME,
        gp.vorname AS VORNAME,
        gp.land AS LAND,
        gp.plz AS PLZ,
        gp.wohnort AS WOHNORT,
        gp.strasse AS STRASSE,
        SUBSTR(gp.kunde_segment_id, 1, 2) AS KUNDE_SEGMENT_ID,
        CASE WHEN gp.prem_segment_id = 1 THEN 'ja' ELSE 'nein' END AS PREM_SEGMENT_ID,
        v.segment_id AS RD_SEGMENT_ID,
        v.letztes_upgrade AS LETZTES_UPGRADE,
        v.vertrag_id_carmen AS VERTRAG_ID_CARMEN,
        v.rechnungsmedium AS RECHNUNGSMEDIUM,
        PARSE_DATE('%d.%m.%Y', '11.11.1111') AS RUECKGEWINN_DATUM, -- to_date
        -- TWIN_MSISDN
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_ms_stat = 'A' THEN bp.tc_msisdn
                    WHEN bp.ms1_stat = 'A' THEN bp.tnv_msisdn -- MS: Rufnummer Masterkarte
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_ms_stat = 'A' THEN bpt.tb_msisdn
                            WHEN bpt.tnv_ms_stat = 'A' THEN bpt.tnv_msisdn
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_ms_stat IS NOT NULL THEN bp.tc_msisdn
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_ms_stat IS NOT NULL THEN bpt.tb_msisdn
                    WHEN bpt.tnv_ms_stat IS NOT NULL THEN bpt.tnv_msisdn
                    ELSE ' '
                END
            ELSE ' '
        END AS TWIN_MSISDN,
        gp.organisationseinheit AS ORGANISATIONSEINHEIT,
        gp.adresszusatz AS ADRESSZUSATZ,
        gp.namenszusatz AS NAMENSZUSATZ,
        IFNULL(bp.data_option_rein, bpt.data_option_rein) AS DATA_OPTION_REIN,
        IFNULL(bp.voice_option_rein, bpt.voice_option_rein) AS VOICE_OPTION_REIN,
        IFNULL(bp.mix_option, bpt.mix_option) AS MIX_OPTION,
        IFNULL(bp.multi_option, bpt.multi_option) AS MULTI_OPTION,
        IFNULL(bp.roaming_option, bpt.roaming_option) AS ROAMING_OPTION,
        IFNULL(bp.sonstige_option, bpt.sonstige_option) AS SONSTIGE_OPTION,
        v.upgradeberechtigt AS UPGRADEBERECHTIGT,
        IFNULL(bp.apn, v.apn) AS APN,
        v.vda AS VDA,
        v.upgradegrund AS UPGRADEGRUND,
        -- E_ID
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_icc_stat = 'A' THEN bp.TNV_E_ID
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat = 'A' THEN bp.TB_E_ID
                            ELSE ' '
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_icc_stat IS NOT NULL THEN bp.TNV_E_ID
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat IS NOT NULL THEN bp.TB_E_ID
                            ELSE ' '
                        END
                END
        END AS E_ID,
        -- CARD_TYPE_NAME
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_icc_stat = 'A' THEN bp.TNV_CARD_TYPE_NAME
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat = 'A' THEN bp.TB_CARD_TYPE_NAME
                            ELSE ' '
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_icc_stat IS NOT NULL THEN bp.TNV_CARD_TYPE_NAME
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat IS NOT NULL THEN bp.TB_CARD_TYPE_NAME
                            ELSE ' '
                        END
                END
        END AS CARD_TYPE_NAME,
        -- LINK_E_ID
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_icc_stat = 'A' THEN bp.TC_E_ID
                    WHEN bp.ms1_stat = 'A' THEN bp.MS1_E_ID
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_icc_stat = 'A' THEN bpt.TB_E_ID
                            WHEN bpt.tnv_icc_stat = 'A' THEN bpt.TNV_E_ID
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_icc_stat IS NOT NULL THEN bp.TC_E_ID
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_icc_stat IS NOT NULL THEN bpt.TB_E_ID
                    WHEN bpt.tnv_icc_stat IS NOT NULL THEN bpt.TNV_E_ID
                    ELSE ' '
                END
            ELSE ' '
        END AS LINK_E_ID,
        -- LINK_CARD_TYPE_NAME
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_icc_stat = 'A' THEN bp.TC_CARD_TYPE_NAME
                    WHEN bp.ms1_stat = 'A' THEN bp.MS1_CARD_TYPE_NAME
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_icc_stat = 'A' THEN bpt.TB_CARD_TYPE_NAME
                            WHEN bpt.tnv_icc_stat = 'A' THEN bpt.TNV_CARD_TYPE_NAME
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_icc_stat IS NOT NULL THEN bp.TC_CARD_TYPE_NAME
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_icc_stat IS NOT NULL THEN bpt.TB_CARD_TYPE_NAME
                    WHEN bpt.tnv_icc_stat IS NOT NULL THEN bpt.TNV_CARD_TYPE_NAME
                    ELSE ' '
                END
            ELSE ' '
        END AS LINK_CARD_TYPE_NAME,
        -- MS2_E_ID
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.MS2_E_ID ELSE ' ' END AS MS2_E_ID,
        -- MS2_CARD_TYPE_NAME
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.MS2_CARD_TYPE_NAME ELSE ' ' END AS MS2_CARD_TYPE_NAME,
        -- ICCID
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_icc_stat = 'A' THEN bp.tnv_iccid
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat = 'A' THEN bp.tb_iccid
                            ELSE ' '
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_icc_stat IS NOT NULL THEN bp.tnv_iccid
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat IS NOT NULL THEN bp.tb_iccid
                            ELSE ' '
                        END
                END
        END AS ICCID,
        -- LINK_ICCID
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_icc_stat = 'A' THEN bp.tc_iccid
                    WHEN bp.ms1_stat = 'A' THEN bp.ms1_iccid
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_icc_stat = 'A' THEN bpt.tb_iccid
                            WHEN bpt.tnv_icc_stat = 'A' THEN bpt.tnv_iccid
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_icc_stat IS NOT NULL THEN bp.tc_iccid
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_icc_stat IS NOT NULL THEN bpt.tb_iccid
                    WHEN bpt.tnv_icc_stat IS NOT NULL THEN bpt.tnv_iccid
                    ELSE ' '
                END
            ELSE ' '
        END AS LINK_ICCID,
        -- MS2_ICCID
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.ms2_iccid ELSE ' ' END AS MS2_ICCID,
        -- HLR
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_icc_stat = 'A' THEN bp.tnv_hlr
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat = 'A' THEN bp.tb_hlr
                            ELSE ' '
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_icc_stat IS NOT NULL THEN bp.tnv_hlr
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat IS NOT NULL THEN bp.tb_hlr
                            ELSE ' '
                        END
                END
        END AS HLR,
        -- LINK_HLR
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_icc_stat = 'A' THEN bp.tc_hlr
                    WHEN bp.ms1_stat = 'A' THEN bp.ms1_hlr
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_icc_stat = 'A' THEN bpt.tb_hlr
                            WHEN bpt.tnv_icc_stat = 'A' THEN bpt.tnv_hlr
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_icc_stat IS NOT NULL THEN bp.tc_hlr
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_icc_stat IS NOT NULL THEN bpt.tb_hlr
                    WHEN bpt.tnv_icc_stat IS NOT NULL THEN bpt.tnv_hlr
                    ELSE ' '
                END
            ELSE ' '
        END AS LINK_HLR,
        -- MS2_HLR
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.ms2_hlr ELSE ' ' END AS MS2_HLR,
        v.sperrart AS SPERRART,
        v.sperrgrund AS SPERRGRUND,
        v.stillegungszeitraum AS STILLEGUNGSZEITRAUM,
        v.twin_vertrag_id AS TWIN_VERTRAG_ID,
        v.cntrct_ty AS CNTRCT_TY,
        dn.firmenname AS DN_FIRMENNAME,
        dn.akad_titel AS DN_AKAD_TITEL,
        dn.nachname AS DN_NACHNAME,
        dn.vorname AS DN_VORNAME,
        dn.land AS DN_LAND,
        dn.plz AS DN_PLZ,
        dn.wohnort AS DN_WOHNORT,
        dn.strasse AS DN_STRASSE,
        dn.organisationseinheit AS DN_ORG_EINHEIT,
        dn.adresszusatz AS DN_ADRESSZUSATZ,
        dn.namenszusatz AS DN_NAMENSZUSATZ,
        ev.firmenname AS EV_FIRMENNAME,
        ev.akad_titel AS EV_AKAD_TITEL,
        ev.nachname AS EV_NACHNAME,
        ev.vorname AS EV_VORNAME,
        ev.land AS EV_LAND,
        ev.plz AS EV_PLZ,
        ev.wohnort AS EV_WOHNORT,
        ev.strasse AS EV_STRASSE,
        ev.organisationseinheit AS EV_ORG_EINHEIT,
        ev.adresszusatz AS EV_ADRESSZUSATZ,
        ev.namenszusatz AS EV_NAMENSZUSATZ,
        v.cost_centre AS KOSTENSTELLE,
        v.cost_centre_user AS KOSTENSTELLENNUTZER,
        IFNULL(bp.bcp_vertrag, bpt.bcp_vertrag) AS BCP_VERTRAG,
        IFNULL(bp.bcp_iccid, bpt.bcp_iccid) AS BCP_ICCID,
        IFNULL(bp.bcp_hlr, bpt.bcp_hlr) AS BCP_HLR,
        gp.mwst_kennzeichen AS GP_MWST_KENNZEICHEN,
        dn.mwst_kennzeichen AS DN_MWST_KENNZEICHEN,
        ev.mwst_kennzeichen AS EV_MWST_KENNZEICHEN,
        v.mwst_kennzeichen AS V_MWST_KENNZEICHEN,
        IFNULL(bp.bcp_tn_tel, bpt.bcp_tn_tel) AS BCP_TN_TEL, -- decode(bp.bcp_vertrag,NULL,bpt.bcp_tn_tel,bp.bcp_tn_tel)
        v.RECHN_INH_KONFIG_TEXT,
        v.commitment_reference_date AS COMMITMENT_REFERENCE_DATE,
        v.cntrct_validity_id AS CNTRCT_VALIDITY_ID,
        v.sv_id AS SV_ID,
        -- MultiSim 3 Plus START
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_iccid ELSE ' ' END AS MS3_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_e_id ELSE ' ' END AS MS3_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_card_type_name ELSE ' ' END AS MS3_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_hlr ELSE ' ' END AS MS3_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_iccid ELSE ' ' END AS MS4_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_e_id ELSE ' ' END AS MS4_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_card_type_name ELSE ' ' END AS MS4_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_hlr ELSE ' ' END AS MS4_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_iccid ELSE ' ' END AS MS5_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_e_id ELSE ' ' END AS MS5_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_card_type_name ELSE ' ' END AS MS5_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_hlr ELSE ' ' END AS MS5_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_iccid ELSE ' ' END AS MS6_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_e_id ELSE ' ' END AS MS6_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_card_type_name ELSE ' ' END AS MS6_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_hlr ELSE ' ' END AS MS6_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_iccid ELSE ' ' END AS MS7_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_e_id ELSE ' ' END AS MS7_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_card_type_name ELSE ' ' END AS MS7_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_hlr ELSE ' ' END AS MS7_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_iccid ELSE ' ' END AS MS8_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_e_id ELSE ' ' END AS MS8_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_card_type_name ELSE ' ' END AS MS8_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_hlr ELSE ' ' END AS MS8_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_iccid ELSE ' ' END AS MS9_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_e_id ELSE ' ' END AS MS9_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_card_type_name ELSE ' ' END AS MS9_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_hlr ELSE ' ' END AS MS9_HLR,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_iccid ELSE ' ' END AS MS10_ICCID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_e_id ELSE ' ' END AS MS10_E_ID,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_card_type_name ELSE ' ' END AS MS10_CARD_TYPE_NAME,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_hlr ELSE ' ' END AS MS10_HLR
    FROM
        `project.source_dataset.sof_ta_p_vertrag` AS v
    JOIN
        `project.source_dataset.sof_ta_p_gesch_part` AS gp
        ON v.twin_vertrag_id = gp.cntrct_id
    LEFT JOIN
        `project.source_dataset.sof_ta_p_basisprod` AS bp
        ON v.vertrag_id_carmen = bp.cntrct_id
    LEFT JOIN
        `project.source_dataset.sof_ta_p_basisprod` AS bpt
        ON v.twin_vertrag_id = bpt.cntrct_id
    LEFT JOIN
        `project.source_dataset.sof_ta_p_evn_empf` AS ev
        ON v.twin_vertrag_id = ev.cntrct_id
    LEFT JOIN
        `project.source_dataset.sof_ta_p_dn_nutzer` AS dn
        ON v.twin_vertrag_id = dn.cntrct_id
    WHERE
        v.cntrct_ty = 20 -- nur Mobilfunkzusatzvertraege (Twin)
    ;

    -- ========================= Step03 ==================================
    -- Aktualisierung der Tabelle RPT$TA_S_D1_RECH_KUNDE
    -- Original Oracle: insert into temp tables, then join and insert into target.
    -- BigQuery: Use CTEs for temporary data.

    CALL `project.admin_dataset.log_message`(p_job_kennung, p_eintrags_nr, 'INFO', 'Updating rpt_ta_s_d1_rech_kunde');

    CREATE OR REPLACE TABLE `project.reporting_dataset.rpt_ta_s_d1_rech_kunde` AS
    WITH sof_ta_rechdef AS (
        SELECT DISTINCT
            RECHDEF_ID_CARMEN,
            KUNDENNUMMER
        FROM
            `project.reporting_dataset.rpt_ta_s_d1_rech_empf`
    ),
    sof_ta_kd_kto AS (
        SELECT DISTINCT
            RECHDEF_ID_CARMEN,
            KUNDENKONTO
        FROM
            `project.reporting_dataset.rpt_ta_s_d1_vertrag`
    )
    SELECT
        kd_kto.KUNDENKONTO,
        rechdef.KUNDENNUMMER
    FROM
        sof_ta_rechdef AS rechdef
    LEFT JOIN
        sof_ta_kd_kto AS kd_kto
        ON rechdef.RECHDEF_ID_CARMEN = kd_kto.RECHDEF_ID_CARMEN
    ;

    -- ========================= Step04 ==================================
    -- Aktualisierung der Tabelle RPT$TA_S_D1_DISCOUNT

    CALL `project.admin_dataset.log_message`(p_job_kennung, p_eintrags_nr, 'INFO', 'Updating rpt_ta_s_d1_discount');

    CREATE OR REPLACE TABLE `project.reporting_dataset.rpt_ta_s_d1_discount` AS
    SELECT
        contract_number AS CONTRACT_NUMBER,
        rabatt_alle AS RABATT_ALLE
    FROM
        `project.source_dataset.sof_ta_p_discount`
    ;

    -- ========================= Step05 ==================================
    -- Aktualisierung der Tabelle RPT$TA_S_D1_DISCOUNT_RR

    CALL `project.admin_dataset.log_message`(p_job_kennung, p_eintrags_nr, 'INFO', 'Updating rpt_ta_s_d1_discount_rr');

    CREATE OR REPLACE TABLE `project.reporting_dataset.rpt_ta_s_d1_discount_rr` AS
    SELECT
        contract_number AS CONTRACT_NUMBER,
        std_vertrag AS STD_VERTRAG,
        rabatt AS RABATT,
        rabattierte_rech_pos AS RABATTIERTE_RECH_POS,
        rabatthoehe AS RABATTHOEHE,
        cntrct_template_id AS CNTRCT_TEMPLATE_ID,
        disc_invoice_item_id AS DISC_INVOICE_ITEM_ID
    FROM
        `project.source_dataset.sof_ta_p_discount_rr`
    ;

    -- ========================= Step06 ==================================
    -- Aktualisierung der Tabelle RPT$TA_S_D1_VPN

    CALL `project.admin_dataset.log_message`(p_job_kennung, p_eintrags_nr, 'INFO', 'Updating rpt_ta_s_d1_vpn');

    CREATE OR REPLACE TABLE `project.reporting_dataset.rpt_ta_s_d1_vpn` AS
    SELECT
        VERTRAGS_ID AS VERTRAG_ID_CARMEN,
        VPN_ID AS VPN_ID
    FROM
        `project.source_dataset.sof_ta_p_d1_vpn`
    ;

    -- Return the number of records from the main contract table update if needed by orchestration
    -- For now, setting a placeholder. In a real scenario, this would be derived from the actual inserts.
    SELECT COUNT(1) INTO v_records FROM `project.reporting_dataset.rpt_ta_s_d1_vertrag`;
    CALL `project.admin_dataset.log_message`(p_job_kennung, p_eintrags_nr, 'INFO', FORMAT('D_AUSD_AUSTAUSCH_SP completed. Processed %d records for rpt_ta_s_d1_vertrag', v_records));

EXCEPTION WHEN ERROR THEN
    CALL `project.admin_dataset.log_message`(p_job_kennung, p_eintrags_nr, 'ERROR', FORMAT('D_AUSD_AUSTAUSCH_SP failed: %s', @@error.message));
    RAISE;
END;
;