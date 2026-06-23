--
-- BigQuery Stored Procedure for the core logic of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
-- Replaces k_ausd_austausch.ksh and d_ausd_austausch.sql
--
-- Parameters:
--   p_stichtag: The snapshot date for data extraction.
--

CREATE OR REPLACE PROCEDURE `bert_reporting`.`k_ausd_austausch_sp`(
    p_stichtag DATE
)
BEGIN
    -- STEP 01: Update rpt_ta_s_d1_rech_empf
    -- Equivalent to Oracle RENAME, TRUNCATE, INSERT, RENAME, DROP INDEX, CREATE INDEX
    -- In BigQuery, this is typically handled by CREATE OR REPLACE TABLE AS SELECT or TRUNCATE + INSERT.
    -- We will use TRUNCATE + INSERT for clarity and to retain table properties if they exist.
    TRUNCATE TABLE `bert_reporting`.`rpt_ta_s_d1_rech_empf`;

    INSERT INTO `bert_reporting`.`rpt_ta_s_d1_rech_empf`
    (
        dwh_konto_id,
        rechdef_id_carmen,
        konto_nr_dpps,
        quelle,
        anrede,
        rechnungsempfaenger,
        zusatz_1,
        zusatz_2,
        strasse,
        plz,
        wohnort,
        bankname,
        kontonummer,
        blz,
        organisationseinheit,
        land,
        firma,
        vorname,
        nachname,
        kundennummer,
        mwst_kennzeichen,
        iban,
        bic
    )
    SELECT
        c.kundenkonto,
        c.rechdef_id,
        c.dpps_kontonummer,
        c.quelle,
        c.akad_titel,
        c.rechnungsempfaenger,
        c.zusatz_1,
        c.zusatz_2,
        SUBSTR(c.strasse, 1, 45),
        c.plz,
        c.wohnort,
        c.bankname,
        c.bank_kontonummer,
        c.blz,
        c.organisationseinheit,
        c.land,
        SUBSTR(c.firma, 1, 40),
        c.vorname,
        c.nachname,
        c.kun_nr_rech_empf,
        c.mwst_kennzeichen,
        c.iban,
        c.bic
    FROM
        `bert_reporting`.`sof_ta_p_rech_empf` AS c;

    -- STEP 02: Update rpt_ta_s_d1_vertrag
    -- Uses a UNION ALL query to populate the contract table.
    TRUNCATE TABLE `bert_reporting`.`rpt_ta_s_d1_vertrag`;

    INSERT INTO `bert_reporting`.`rpt_ta_s_d1_vertrag`
    (
        kund_nr_dpps,
        partner_id_carmen,
        rechdef_id_carmen,
        kundenkonto,
        waehrung,
        rahmenvertrag_id,
        rechnungslauf,
        vo_kenn,
        geplant_kuend,
        eingang_kuend,
        rv_akz,
        vertragsbeginn,
        order_number,
        vertragsstatus,
        twincard,
        msisdn,
        dwh_tarifgr_text,
        bindefrist,
        vertragsbindung,
        rechnungszahlart,
        evn,
        data96,
        fax,
        firmenname,
        akad_titel,
        nachname,
        vorname,
        land,
        plz,
        wohnort,
        strasse,
        kunde_segment_id,
        prem_segment_id,
        rd_segment_id,
        letztes_upgrade,
        vertrag_id_carmen,
        rechnungsmedium,
        rueckgewinn_datum,
        twin_msisdn,
        organisationseinheit,
        adresszusatz,
        namenszusatz,
        data_option_rein,
        voice_option_rein,
        mix_option,
        multi_option,
        roaming_option,
        sonstige_option,
        upgradeberechtigt,
        apn,
        vda,
        upgradegrund,
        e_id,
        card_type_name,
        link_e_id,
        link_card_type_name,
        ms2_e_id,
        ms2_card_type_name,
        iccid,
        link_iccid,
        ms2_iccid,
        hlr,
        link_hlr,
        ms2_hlr,
        sperrart,
        sperrgrund,
        stillegungszeitraum,
        twin_vertrag_id,
        cntrct_ty,
        dn_firmenname,
        dn_akad_titel,
        dn_nachname,
        dn_vorname,
        dn_land,
        dn_plz,
        dn_wohnort,
        dn_strasse,
        dn_org_einheit,
        dn_adresszusatz,
        dn_namenszusatz,
        ev_firmenname,
        ev_akad_titel,
        ev_nachname,
        ev_vorname,
        ev_land,
        ev_plz,
        ev_wohnort,
        ev_strasse,
        ev_org_einheit,
        ev_adresszusatz,
        ev_namenszusatz,
        kostenstelle,
        kostenstellennutzer,
        bcp_vertrag,
        bcp_iccid,
        bcp_hlr,
        gp_mwst_kennzeichen,
        dn_mwst_kennzeichen,
        ev_mwst_kennzeichen,
        v_mwst_kennzeichen,
        bcp_tn_tel,
        rechn_inh_konfig_text,
        commitment_reference_date,
        cntrct_validity_id,
        sv_id,
        ms3_iccid, ms3_e_id, ms3_card_type_name, ms3_hlr,
        ms4_iccid, ms4_e_id, ms4_card_type_name, ms4_hlr,
        ms5_iccid, ms5_e_id, ms5_card_type_name, ms5_hlr,
        ms6_iccid, ms6_e_id, ms6_card_type_name, ms6_hlr,
        ms7_iccid, ms7_e_id, ms7_card_type_name, ms7_hlr,
        ms8_iccid, ms8_e_id, ms8_card_type_name, ms8_hlr,
        ms9_iccid, ms9_e_id, ms9_card_type_name, ms9_hlr,
        ms10_iccid, ms10_e_id, ms10_card_type_name, ms10_hlr
    )
    SELECT
        gp.tm_kundennummer,
        v.partner_id_carmen,
        v.rechdef_id_carmen,
        v.kundenkonto,
        'EUR',
        SUBSTR(v.rahmenvertrag_id, 1, 10),
        v.rechnungslauf,
        CAST(v.vo_kenn AS STRING),
        v.geplant_kuend,
        v.eingang_kuend,
        v.rv_action_id,
        v.vertragsbeginn,
        v.order_number,
        v.vertragsstatus,
        -- twincard
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
        END,
        -- msisdn
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
        END,
        v.dwh_tarifgr_text,
        v.bindefrist,
        v.vertragsbindung,
        v.rechnungszahlart,
        -- evn
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
        END,
        -- data96
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
        END,
        -- fax
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
        END,
        gp.firmenname,
        gp.akad_titel,
        gp.nachname,
        gp.vorname,
        gp.land,
        gp.plz,
        gp.wohnort,
        gp.strasse,
        SUBSTR(gp.kunde_segment_id, 1, 2),
        CASE gp.prem_segment_id
            WHEN 1 THEN 'ja'
            ELSE 'nein'
        END,
        v.segment_id,
        v.letztes_upgrade,
        v.vertrag_id_carmen,
        v.rechnungsmedium,
        PARSE_DATE('%d.%m.%Y', '11.11.1111'), -- Legacy fixed date
        -- twin_msisdn
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
        END,
        gp.organisationseinheit,
        gp.adresszusatz,
        gp.namenszusatz,
        COALESCE(bp.data_option_rein, bpt.data_option_rein),
        COALESCE(bp.voice_option_rein, bpt.voice_option_rein),
        COALESCE(bp.mix_option, bpt.mix_option),
        COALESCE(bp.multi_option, bpt.multi_option),
        COALESCE(bp.roaming_option, bpt.roaming_option),
        COALESCE(bp.sonstige_option, bpt.sonstige_option),
        v.upgradeberechtigt,
        COALESCE(bp.apn, v.apn),
        v.vda,
        v.upgradegrund,
        -- E_ID
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_icc_stat = 'A' THEN bp.tnv_e_id
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat = 'A' THEN bp.tb_e_id
                            ELSE ' '
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_icc_stat IS NOT NULL THEN bp.tnv_e_id
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat IS NOT NULL THEN bp.tb_e_id
                            ELSE ' '
                        END
                END
        END,
        -- CARD_TYPE_NAME
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_icc_stat = 'A' THEN bp.tnv_card_type_name
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat = 'A' THEN bp.tb_card_type_name
                            ELSE ' '
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_icc_stat IS NOT NULL THEN bp.tnv_card_type_name
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat IS NOT NULL THEN bp.tb_card_type_name
                            ELSE ' '
                        END
                END
        END,
        -- LINK_E_ID
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_icc_stat = 'A' THEN bp.tc_e_id
                    WHEN bp.ms1_stat = 'A' THEN bp.ms1_e_id
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_icc_stat = 'A' THEN bpt.tb_e_id
                            WHEN bpt.tnv_icc_stat = 'A' THEN bpt.tnv_e_id
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_icc_stat IS NOT NULL THEN bp.tc_e_id
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_icc_stat IS NOT NULL THEN bpt.tb_e_id
                    WHEN bpt.tnv_icc_stat IS NOT NULL THEN bpt.tnv_e_id
                    ELSE ' '
                END
            ELSE ' '
        END,
        -- LINK_CARD_TYPE_NAME
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_icc_stat = 'A' THEN bp.tc_card_type_name
                    WHEN bp.ms1_stat = 'A' THEN bp.ms1_card_type_name
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_icc_stat = 'A' THEN bpt.tb_card_type_name
                            WHEN bpt.tnv_icc_stat = 'A' THEN bpt.tnv_card_type_name
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_icc_stat IS NOT NULL THEN bp.tc_card_type_name
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_icc_stat IS NOT NULL THEN bpt.tb_card_type_name
                    WHEN bpt.tnv_icc_stat IS NOT NULL THEN bpt.tnv_card_type_name
                    ELSE ' '
                END
            ELSE ' '
        END,
        -- MS2_E_ID
        CASE
            WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.ms2_e_id
            ELSE ' '
        END,
        -- MS2_CARD_TYPE_NAME
        CASE
            WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.ms2_card_type_name
            ELSE ' '
        END,
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
        END,
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
        END,
        -- MS2_ICCID
        CASE
            WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.ms2_iccid
            ELSE ' '
        END,
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
        END,
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
        END,
        -- MS2_HLR
        CASE
            WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.ms2_hlr
            ELSE ' '
        END,
        v.sperrart,
        v.sperrgrund,
        v.stillegungszeitraum,
        v.twin_vertrag_id,
        v.cntrct_ty,
        dn.firmenname,
        dn.akad_titel,
        dn.nachname,
        dn.vorname,
        dn.land,
        dn.plz,
        dn.wohnort,
        dn.strasse,
        dn.organisationseinheit,
        dn.adresszusatz,
        dn.namenszusatz,
        ev.firmenname,
        ev.akad_titel,
        ev.nachname,
        ev.vorname,
        ev.land,
        ev.plz,
        ev.wohnort,
        ev.strasse,
        ev.organisationseinheit,
        ev.adresszusatz,
        ev.namenszusatz,
        v.cost_centre,
        v.cost_centre_user,
        COALESCE(bp.bcp_vertrag, bpt.bcp_vertrag),
        COALESCE(bp.bcp_iccid, bpt.bcp_iccid),
        COALESCE(bp.bcp_hlr, bpt.bcp_hlr),
        gp.mwst_kennzeichen,
        dn.mwst_kennzeichen,
        ev.mwst_kennzeichen,
        v.mwst_kennzeichen,
        COALESCE(bp.bcp_vertrag, bpt.bcp_tn_tel, bp.bcp_tn_tel), -- Logic `decode(bp.bcp_vertrag,NULL,bpt.bcp_tn_tel,bp.bcp_tn_tel)` translated to COALESCE.
        v.rechn_inh_konfig_text,
        v.commitment_reference_date,
        v.cntrct_validity_id,
        v.sv_id,
        -- MultiSIM 3 Plus START
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_hlr ELSE ' ' END
        -- MultiSIM 3 Plus END
    FROM
        `bert_reporting`.`sof_ta_p_vertrag` AS v
    JOIN
        `bert_reporting`.`sof_ta_p_gesch_part` AS gp
        ON v.vertrag_id_carmen = gp.cntrct_id
    LEFT JOIN
        `bert_reporting`.`sof_ta_p_basisprod` AS bp
        ON v.vertrag_id_carmen = bp.cntrct_id
    LEFT JOIN
        `bert_reporting`.`sof_ta_p_basisprod` AS bpt
        ON v.twin_vertrag_id = bpt.cntrct_id
    LEFT JOIN
        `bert_reporting`.`sof_ta_p_evn_empf` AS ev
        ON v.vertrag_id_carmen = ev.cntrct_id
    LEFT JOIN
        `bert_reporting`.`sof_ta_p_dn_nutzer` AS dn
        ON v.vertrag_id_carmen = dn.cntrct_id
    WHERE
        v.cntrct_ty <> 11 -- keine RV-Zusatzvertraege
        AND v.cntrct_ty <> 20 -- keine Mobilfunkzusatzvertraege

    UNION ALL

    SELECT
        gp.tm_kundennummer,
        v.partner_id_carmen,
        v.rechdef_id_carmen,
        v.kundenkonto,
        'EUR',
        SUBSTR(v.rahmenvertrag_id, 1, 10),
        v.rechnungslauf,
        CAST(v.vo_kenn AS STRING),
        v.geplant_kuend,
        v.eingang_kuend,
        v.rv_action_id,
        v.vertragsbeginn,
        v.order_number,
        v.vertragsstatus,
        -- twincard
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
        END,
        -- msisdn
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
        END,
        v.dwh_tarifgr_text,
        v.bindefrist,
        v.vertragsbindung,
        v.rechnungszahlart,
        -- evn
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
            WHEN 32 THEN 'Komfort-Plus / separater EVN (Komfort-Plus)'
            WHEN 13 THEN 'Komfort-Plus / separater EVN (Standard)'
            WHEN 23 THEN 'Komfort-Plus / separater EVN (Komfort)'
            WHEN 33 THEN 'Komfort-Plus / separater EVN (Komfort-Plus)'
            WHEN 14 THEN 'Standard-Plus / separater EVN (Standard)'
            WHEN 24 THEN 'Standard-Plus / separater EVN (Komfort)'
            WHEN 34 THEN 'Standard-Plus / separater EVN (Komfort-Plus)'
            ELSE 'nein'
        END,
        -- data96
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
        END,
        -- fax
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
        END,
        gp.firmenname,
        gp.akad_titel,
        gp.nachname,
        gp.vorname,
        gp.land,
        gp.plz,
        gp.wohnort,
        gp.strasse,
        SUBSTR(gp.kunde_segment_id, 1, 2),
        CASE gp.prem_segment_id
            WHEN 1 THEN 'ja'
            ELSE 'nein'
        END,
        v.segment_id,
        v.letztes_upgrade,
        v.vertrag_id_carmen,
        v.rechnungsmedium,
        PARSE_DATE('%d.%m.%Y', '11.11.1111'), -- Legacy fixed date
        -- twin_msisdn
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_ms_stat = 'A' THEN bp.tc_msisdn
                    WHEN bp.ms1_stat = 'A' THEN bp.tnv_msisdn
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
        END,
        gp.organisationseinheit,
        gp.adresszusatz,
        gp.namenszusatz,
        COALESCE(bp.data_option_rein, bpt.data_option_rein),
        COALESCE(bp.voice_option_rein, bpt.voice_option_rein),
        COALESCE(bp.mix_option, bpt.mix_option),
        COALESCE(bp.multi_option, bpt.multi_option),
        COALESCE(bp.roaming_option, bpt.roaming_option),
        COALESCE(bp.sonstige_option, bpt.sonstige_option),
        v.upgradeberechtigt,
        COALESCE(bp.apn, v.apn),
        v.vda,
        v.upgradegrund,
        -- E_ID
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_icc_stat = 'A' THEN bp.tnv_e_id
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat = 'A' THEN bp.tb_e_id
                            ELSE ' '
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_icc_stat IS NOT NULL THEN bp.tnv_e_id
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat IS NOT NULL THEN bp.tb_e_id
                            ELSE ' '
                        END
                END
        END,
        -- CARD_TYPE_NAME
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tnv_icc_stat = 'A' THEN bp.tnv_card_type_name
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat = 'A' THEN bp.tb_card_type_name
                            ELSE ' '
                        END
                END
            ELSE
                CASE
                    WHEN bp.tnv_icc_stat IS NOT NULL THEN bp.tnv_card_type_name
                    ELSE
                        CASE
                            WHEN bp.tb_icc_stat IS NOT NULL THEN bp.tb_card_type_name
                            ELSE ' '
                        END
                END
        END,
        -- LINK_E_ID
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_icc_stat = 'A' THEN bp.tc_e_id
                    WHEN bp.ms1_stat = 'A' THEN bp.ms1_e_id
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_icc_stat = 'A' THEN bpt.tb_e_id
                            WHEN bpt.tnv_icc_stat = 'A' THEN bpt.tnv_e_id
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_icc_stat IS NOT NULL THEN bp.tc_e_id
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_icc_stat IS NOT NULL THEN bpt.tb_e_id
                    WHEN bpt.tnv_icc_stat IS NOT NULL THEN bpt.tnv_e_id
                    ELSE ' '
                END
            ELSE ' '
        END,
        -- LINK_CARD_TYPE_NAME
        CASE
            WHEN v.vertragsstatus = 'A' THEN
                CASE
                    WHEN bp.tc_icc_stat = 'A' THEN bp.tc_card_type_name
                    WHEN bp.ms1_stat = 'A' THEN bp.ms1_card_type_name
                    WHEN v.twincard = 'TB' THEN
                        CASE
                            WHEN bpt.tb_icc_stat = 'A' THEN bpt.tb_card_type_name
                            WHEN bpt.tnv_icc_stat = 'A' THEN bpt.tnv_card_type_name
                            ELSE ' '
                        END
                    ELSE ' '
                END
            WHEN bp.tc_icc_stat IS NOT NULL THEN bp.tc_card_type_name
            WHEN v.twincard = 'TB' THEN
                CASE
                    WHEN bpt.tb_icc_stat IS NOT NULL THEN bpt.tb_card_type_name
                    WHEN bpt.tnv_icc_stat IS NOT NULL THEN bpt.tnv_card_type_name
                    ELSE ' '
                END
            ELSE ' '
        END,
        -- MS2_E_ID
        CASE
            WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.ms2_e_id
            ELSE ' '
        END,
        -- MS2_CARD_TYPE_NAME
        CASE
            WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.ms2_card_type_name
            ELSE ' '
        END,
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
        END,
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
        END,
        -- MS2_ICCID
        CASE
            WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.ms2_iccid
            ELSE ' '
        END,
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
        END,
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
        END,
        -- MS2_HLR
        CASE
            WHEN v.vertragsstatus = 'A' AND bp.ms2_stat = 'A' THEN bp.ms2_hlr
            ELSE ' '
        END,
        v.sperrart,
        v.sperrgrund,
        v.stillegungszeitraum,
        v.twin_vertrag_id,
        v.cntrct_ty,
        dn.firmenname,
        dn.akad_titel,
        dn.nachname,
        dn.vorname,
        dn.land,
        dn.plz,
        dn.wohnort,
        dn.strasse,
        dn.organisationseinheit,
        dn.adresszusatz,
        dn.namenszusatz,
        ev.firmenname,
        ev.akad_titel,
        ev.nachname,
        ev.vorname,
        ev.land,
        ev.plz,
        ev.wohnort,
        ev.strasse,
        ev.organisationseinheit,
        ev.adresszusatz,
        ev.namenszusatz,
        v.cost_centre,
        v.cost_centre_user,
        COALESCE(bp.bcp_vertrag, bpt.bcp_vertrag),
        COALESCE(bp.bcp_iccid, bpt.bcp_iccid),
        COALESCE(bp.bcp_hlr, bpt.bcp_hlr),
        gp.mwst_kennzeichen,
        dn.mwst_kennzeichen,
        ev.mwst_kennzeichen,
        v.mwst_kennzeichen,
        COALESCE(bp.bcp_vertrag, bpt.bcp_tn_tel, bp.bcp_tn_tel), -- Logic `decode(bp.bcp_vertrag,NULL,bpt.bcp_tn_tel,bp.bcp_tn_tel)` translated to COALESCE.
        v.rechn_inh_konfig_text,
        v.commitment_reference_date,
        v.cntrct_validity_id,
        v.sv_id,
        -- MultiSIM 3 Plus START
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms3_stat = 'A' THEN bp.ms3_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms4_stat = 'A' THEN bp.ms4_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms5_stat = 'A' THEN bp.ms5_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms6_stat = 'A' THEN bp.ms6_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms7_stat = 'A' THEN bp.ms7_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms8_stat = 'A' THEN bp.ms8_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms9_stat = 'A' THEN bp.ms9_hlr ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_iccid ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_e_id ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_card_type_name ELSE ' ' END,
        CASE WHEN v.vertragsstatus = 'A' AND bp.ms10_stat = 'A' THEN bp.ms10_hlr ELSE ' ' END
        -- MultiSIM 3 Plus END
    FROM
        `bert_reporting`.`sof_ta_p_vertrag` AS v
    JOIN
        `bert_reporting`.`sof_ta_p_gesch_part` AS gp
        ON v.twin_vertrag_id = gp.cntrct_id
    LEFT JOIN
        `bert_reporting`.`sof_ta_p_basisprod` AS bp
        ON v.vertrag_id_carmen = bp.cntrct_id
    LEFT JOIN
        `bert_reporting`.`sof_ta_p_basisprod` AS bpt
        ON v.twin_vertrag_id = bpt.cntrct_id
    LEFT JOIN
        `bert_reporting`.`sof_ta_p_evn_empf` AS ev
        ON v.twin_vertrag_id = ev.cntrct_id
    LEFT JOIN
        `bert_reporting`.`sof_ta_p_dn_nutzer` AS dn
        ON v.twin_vertrag_id = dn.cntrct_id
    WHERE
        v.cntrct_ty = 20; -- nur Mobilfunkzusatzvertraege (Twin)

    -- STEP 03: Update rpt_ta_s_d1_rech_kunde
    -- Uses temporary intermediate tables, translated to CTEs.
    TRUNCATE TABLE `bert_reporting`.`rpt_ta_s_d1_rech_kunde`;

    INSERT INTO `bert_reporting`.`rpt_ta_s_d1_rech_kunde`
    (
        kundenkonto,
        kundennummer
    )
    WITH
        sof_ta_rechdef AS (
            SELECT DISTINCT
                re.rechdef_id_carmen,
                re.kundennummer
            FROM
                `bert_reporting`.`rpt_ta_s_d1_rech_empf` AS re
        ),
        sof_ta_kd_kto AS (
            SELECT DISTINCT
                ve.rechdef_id_carmen,
                ve.kundenkonto
            FROM
                `bert_reporting`.`rpt_ta_s_d1_vertrag` AS ve
        )
    SELECT
        ve.kundenkonto,
        re.kundennummer
    FROM
        sof_ta_rechdef AS re
    LEFT JOIN
        sof_ta_kd_kto AS ve
        ON re.rechdef_id_carmen = ve.rechdef_id_carmen;

    -- STEP 04: Update rpt_ta_s_d1_discount
    TRUNCATE TABLE `bert_reporting`.`rpt_ta_s_d1_discount`;

    INSERT INTO `bert_reporting`.`rpt_ta_s_d1_discount`
    (
        contract_number,
        rabatt_alle
    )
    SELECT
        contract_number,
        rabatt_alle
    FROM
        `bert_reporting`.`sof_ta_p_discount`;

    -- STEP 05: Update rpt_ta_s_d1_discount_rr
    TRUNCATE TABLE `bert_reporting`.`rpt_ta_s_d1_discount_rr`;

    INSERT INTO `bert_reporting`.`rpt_ta_s_d1_discount_rr`
    (
        contract_number,
        std_vertrag,
        rabatt,
        rabattierte_rech_pos,
        rabatthoehe,
        cntrct_template_id,
        disc_invoice_item_id
    )
    SELECT
        contract_number,
        std_vertrag,
        rabatt,
        rabattierte_rech_pos,
        rabatthoehe,
        cntrct_template_id,
        disc_invoice_item_id
    FROM
        `bert_reporting`.`sof_ta_p_discount_rr`;

    -- STEP 06: Update rpt_ta_s_d1_vpn
    TRUNCATE TABLE `bert_reporting`.`rpt_ta_s_d1_vpn`;

    INSERT INTO `bert_reporting`.`rpt_ta_s_d1_vpn`
    (
        vertrag_id_carmen,
        vpn_id
    )
    SELECT
        vertrags_id,
        vpn_id
    FROM
        `bert_reporting`.`sof_ta_p_d1_vpn`;

END;