-- Migrated from legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_p_basisprod.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh

-- This file contains the original Oracle SQL content from d_ausd_bp_ta_p_basisprod.sql.
-- It requires manual translation and adaptation to BigQuery SQL syntax, data types,
-- and functions. The placeholder in the stored procedure `sp_k_ausd_bp_ta_p_basisprod`
-- should be replaced with the BigQuery-compliant version of this script.

-- ===================================================================
-- datei:  d_ausd_basisprodukt.sql
-- datum:  22.11.2001
-- autor:  andre loebbers (al)
-- ===================================================================
--
-- modifikationen
----------------------------------------------------------------------
-- version datum    autor dokumentation
-- 2.0.4   20011122 al    aufsetzend auf rel2.0.3
-- 2.0.5   20020222 al    drop table nach oben gesetzt, damit kein
--                        abbruch proviziert wird.
-- 2.0.7   20020502 al    twinmsisdn hinzugefuegt
-- 3.0.0   20021031 sj    umstellung auf den crs
-- 3.1.0   20030109 sj    tabellennamenerweiterung um das tagesdatum
--                        und Bercksichtigung der terminierten
--                        MSISDN's bzw. ICC_ID's
-- 6.5.0   20031010 sj    Umstellung auf 6.5 und Aufnahme weiter BP's
-- 7.0.0   20040408 Roh   Tabelle PDS$TA_BPR_INSTANCE wird zu 7.0
--                        in  PDS$TA_BPRI_COM (bpr_ty <> 1)
--                        und PDS$TA_BPRI_NET (bpr_ty =  1) geteilt
-- 7.0.3   20040629 Roh   neue Basisprodukt-IDs
-- 7.0.5   20040720 Roh   MSISDN des BCP-Vertrages (nur voice)
-- 7.5.0   20040831 Roh   Umstellung auf parallel degree 4
--         20040917 Roh   Ausweisung Bevorrechtigung gem. TKSiV (2917)
-- ab 2005 neue Releasenummern
-- 5.1.0   20050110 Roh   4 neue Basisprodukte (3450,3528,3529,3530)
-- 5.1.1   20050209 Roh   4 neue Basisprodukte (3519,3520,3521,3522)
--         20050615 Roh   9 neue Basisprodukte
-- 6.1.0   20060131 Roh   weitere BP
-- 6.2.0   20060306 Roh   weitere BP zu 6.2
-- 6.2.0   20060505 YP    einbau nologging
-- 6.4.0   20061121 RR    Bestimmung Substitutions-Variable v_datum aus
--                        Meldungstabelle (Eintrag BERT_DROP_TEMP_TABLE)
-- 6.4.1   20061124 RR    berflssige ANALYZE/STATISTICS Kommandos entfernt
-- 6.4.2   20070111 ME    Erweiterung um MultiSIM, BPR-ID 3848:
--                        aus icc: 2*7 zustzliche Felder fr Slavekarten 1 und 2
--                        aus msi: 2*3 zustzliche Felder fr Slavekarten 1 und 2
-- 10.2.1  20100428  Alicja Kubicka     CREATE TABLE...AS -> INSERT by SELECT, DROP TABLE -> TRUNCATE TABLE, &v_datum aus den Tabellename entfernt
-- 10.4.1  20101117  Michal Pluta -  business_option,sonstige_option,gprs_option durch DATA_OPTION_REIN,
--                                  VOICE_OPTION_REIN, MIX_OPTION, MULTI_OPTION, ROAMING_OPTION, SONSTIGE_OPTION ersetzen
-- 13.3    20130926 Jaroslaw Wesolowski - INM21571440
-- 17.1    20160728  Terry  Added for sim evolution
-- 17.2.0  20170103 Terry David - new slaves fields added for multisim demand from 3 to 10
-- 17.3.0  20170720 Magdalena Cybula Performance-Optimierung Basisprodukt-Verarbeitung (IM0016750009)
----------------------------------------------------------------------

-- ========================= Step00 ==================================

-- Variable definition (DEFINE v_carmen, COLUMN s_datum new_value v_datum noprint, SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';)
-- These Oracle specific constructs need to be replaced by BigQuery DECLARE/SET or equivalent logic.
-- The v_datum variable is used in table names in older versions, but notes indicate it's removed in 10.2.1.
-- The purpose of this step is to determine a date, potentially for historization or specific table naming.
-- In BigQuery, you would typically pass dates as parameters or use CURRENT_DATE().

-- ************** trace ********************
-- start ../trace.sql.cfg
-- spool ./tmp/trace_d_ausd_basisprod.trc
-- BigQuery does not have direct equivalents for SQL*Plus tracing or spooling.
-- Logging and auditing would be handled via `job_audit_table` as designed.

-- ************** SETTINGS ********************
-- WHENEVER SQLERROR CONTINUE / WHENEVER SQLERROR EXIT FAILURE
-- set timing on
-- These are SQL*Plus client settings. Error handling is done via BigQuery PROCEDURE EXCEPTION blocks.
-- Timing can be derived from start/end timestamps in the audit table.

-- ========================= Step01 ==================================

-- prompt step01: lschen der temporren-tabellen...
-- whenever sqlerror continue
-- lschen der aktuellen tabellen fr den fall eines restarts am gleichen tag

-- begin
-- isbert_schema.dwpa_util_skript.runstatement(0,'TRUNCATE TABLE sof$ta_p_basisprod REUSE STORAGE');
-- end;
-- /
-- The `TRUNCATE TABLE sof$ta_p_basisprod REUSE STORAGE` is directly translated to `TRUNCATE TABLE project.dataset.sof_ta_p_basisprod;`
-- and is included in the stored procedure before the INSERT.

-- whenever sqlerror exit failure


-- ========================= Step12 ==================================

-- prompt step12: zusammenfhrung der zwischentabellen zur rpt$ta_p_basisprodukt...
--------------------------------------------------------------------------------

INSERT /*+ APPEND */ INTO sof$ta_p_basisprod
(CNTRCT_ID,
  EVN,
  TNV_ICCID,
  TNV_MCC,
  TNV_MNC,
  TNV_HLR,
  TNV_SI,
  TNV_ICC_STAT,
  TNV_ICC_VALID,
  TC_ICCID,
  TC_MCC,
  TC_MNC,
  TC_HLR,
  TC_SI,
  TC_ICC_STAT,
  TC_ICC_VALID,
  TB_ICCID,
  TB_MCC,
  TB_MNC,
  TB_HLR,
  TB_SI,
  TB_ICC_STAT,
  TB_ICC_VALID,
  MS1_ICCID,
  MS1_MCC,
  MS1_MNC,
  MS1_HLR,
  MS1_SI,
  MS1_STAT,
  MS1_VALID,
  MS2_ICCID,
  MS2_MCC,
  MS2_MNC,
  MS2_HLR,
  MS2_SI,
  MS2_STAT,
  MS2_VALID,
  TNV_E_ID,
  TNV_CARD_TYPE_NAME,
  TC_E_ID,
  TC_CARD_TYPE_NAME,
  TB_E_ID,
  TB_CARD_TYPE_NAME,
  MS1_E_ID,
  MS1_CARD_TYPE_NAME,
  MS2_E_ID,
  MS2_CARD_TYPE_NAME ,
  TNV_MULTI_SINGLE,
  TC_MULTI_SINGLE,
  TB_MULTI_SINGLE,
  TNV_MSISDN,
  TNV_MS_STAT,
  TNV_MS_VALID,
  TNV_DAT_MSISDN,
  TNV_DAT_STAT,
  TNV_DAT_VALID,
  TNV_FAX_MSISDN,
  TNV_FAX_STAT,
  TNV_FAX_VALID,
  TC_MSISDN,
  TC_MS_STAT,
  TC_MS_VALID,
  TC_DAT_MSISDN,
  TC_DAT_STAT,
  TC_DAT_VALID ,
  TC_FAX_MSISDN,
  TC_FAX_STAT,
  TC_FAX_VALID ,
  TB_MSISDN,
  TB_MS_STAT,
  TB_MS_VALID,
  TB_DAT_MSISDN,
  TB_DAT_STAT,
  TB_DAT_VALID,
  TB_FAX_MSISDN,
  TB_FAX_STAT,
  TB_FAX_VALID ,
  MS1_MSISDN,
  MS1_MS_STAT,
  MS1_MS_VALID,
  MS2_MSISDN,
  MS2_MS_STAT,
  MS2_MS_VALID ,
  DA_MSISDN,
  DA_MS_STAT,
  DA_MS_VALID,
  VDA_MSISDN,
  VDA_MS_STAT ,
  VDA_MS_VALID ,
  TK_MSISDN,
  TK_MS_STAT,
  TK_MS_VALID,
  BCP_VERTRAG,
  BCP_ICCID,
  BCP_HLR,
  APN,
  BCP_TN_TEL,
  DATA_OPTION_REIN,
  VOICE_OPTION_REIN,
  MIX_OPTION,
  MULTI_OPTION ,
  ROAMING_OPTION,
  SONSTIGE_OPTION,
MS3_ICCID,MS3_E_ID,MS3_CARD_TYPE_NAME,MS3_MCC,MS3_MNC,MS3_HLR,MS3_SI,MS3_STAT,MS3_VALID,
MS4_ICCID,MS4_E_ID,MS4_CARD_TYPE_NAME,MS4_MCC,MS4_MNC,MS4_HLR,MS4_SI,MS4_STAT,MS4_VALID,
MS5_ICCID,MS5_E_ID,MS5_CARD_TYPE_NAME,MS5_MCC,MS5_MNC,MS5_HLR,MS5_SI,MS5_STAT,MS5_VALID,
MS6_ICCID,MS6_E_ID,MS6_CARD_TYPE_NAME,MS6_MCC,MS6_MNC,MS6_HLR,MS6_SI,MS6_STAT,MS6_VALID,
MS7_ICCID,MS7_E_ID,MS7_CARD_TYPE_NAME,MS7_MCC,MS7_MNC,MS7_HLR,MS7_SI,MS7_STAT,MS7_VALID,
MS8_ICCID,MS8_E_ID,MS8_CARD_TYPE_NAME,MS8_MCC,MS8_MNC,MS8_HLR,MS8_SI,MS8_STAT,MS8_VALID,
MS9_ICCID,MS9_E_ID,MS9_CARD_TYPE_NAME,MS9_MCC,MS9_MNC,MS9_HLR,MS9_SI,MS9_STAT,MS9_VALID,
MS10_ICCID,MS10_E_ID,MS10_CARD_TYPE_NAME,MS10_MCC,MS10_MNC,MS10_HLR,MS10_SI,MS10_STAT,MS10_VALID
 )
SELECT /*+ ORDERED
           NO_SWAP_JOIN_INPUTS(cn) FULL(cn) parallel(cn,4) NO_SWAP_JOIN_INPUTS(ev) FULL(ev) parallel(ev,4)
           NO_SWAP_JOIN_INPUTS(icc) FULL(icc) parallel(icc,4) NO_SWAP_JOIN_INPUTS(msi) FULL(msi) parallel(msi,4)
           NO_SWAP_JOIN_INPUTS(opt) FULL(opt) parallel(opt,4) NO_SWAP_JOIN_INPUTS(av) FULL(av) parallel(av,4)
           NO_SWAP_JOIN_INPUTS(msd) FULL(msd) parallel(msd,4) NO_SWAP_JOIN_INPUTS(bccm) FULL(bccm) parallel(bccm,4)  */
        cn.cntrct_id,
        ev.evn,
        icc.tn_iccid    as tnv_iccid,
        icc.tn_imsi_mcc as tnv_mcc,
        icc.tn_imsi_mnc as tnv_mnc,
        icc.tn_imsi_hlr as tnv_hlr,
        icc.tn_imsi_si  as tnv_si,
        icc.tn_status   as tnv_icc_stat,
        icc.tn_valid_to as tnv_icc_valid,
        icc.tc_iccid    as tc_iccid,
        icc.tc_imsi_mcc as tc_mcc,
        icc.tc_imsi_mnc as tc_mnc,
        icc.tc_imsi_hlr as tc_hlr,
        icc.tc_imsi_si  as tc_si,
        icc.tc_status   as tc_icc_stat,
        icc.tc_valid_to as tc_icc_valid,
        icc.tb_iccid    as tb_iccid,
        icc.tb_imsi_mcc as tb_mcc,
        icc.tb_imsi_mnc as tb_mnc,
        icc.tb_imsi_hlr as tb_hlr,
        icc.tb_imsi_si  as tb_si,
        icc.tb_status   as tb_icc_stat,
        icc.tb_valid_to as tb_icc_valid,
        -- MultiSIM: Feldumbenennungen analog zu tn_/tc_/tb_:
        icc.ms1_iccid     as ms1_iccid,       -- 20070111 ME
        icc.ms1_imsi_mcc  as ms1_mcc,
        icc.ms1_imsi_mnc  as ms1_mnc,
        icc.ms1_imsi_hlr  as ms1_hlr,
        icc.ms1_imsi_si   as ms1_si,
        icc.ms1_status    as ms1_stat,
        icc.ms1_valid_to  as ms1_valid,
        icc.ms2_iccid     as ms2_iccid,
        icc.ms2_imsi_mcc  as ms2_mcc,
        icc.ms2_imsi_mnc  as ms2_mnc,
        icc.ms2_imsi_hlr  as ms2_hlr,
        icc.ms2_imsi_si   as ms2_si,
        icc.ms2_status    as ms2_stat,
        icc.ms2_valid_to  AS ms2_valid,
        icc.tn_e_id       AS tnv_e_id,
        icc.tn_card_type_name AS tnv_card_type_name,
        icc.tc_e_id       AS tc_e_id,
        icc.tc_card_type_name AS tc_card_type_name,
        icc.tb_e_id       AS tb_e_id,
        icc.tb_card_type_name AS tb_card_type_name,
        icc.ms1_e_id      AS ms1_e_id,
        icc.ms1_card_type_name AS ms1_card_type_name,
        icc.ms2_e_id      AS ms2_e_id,
        icc.ms2_card_type_name AS ms2_card_type_name,
        msi.tn_multi_single as tnv_multi_single,
        msi.tc_multi_single as tc_multi_single,
        msi.tb_multi_single as tb_multi_single,
        msi.tn_tel_msisdn   as tnv_msisdn,
        msi.tn_tel_status   as tnv_ms_stat,
        msi.tn_tel_valid_to as tnv_ms_valid,
        msi.tn_dat_msisdn   as tnv_dat_msisdn,
        msi.tn_dat_status   as tnv_dat_stat,
        msi.tn_dat_valid_to as tnv_dat_valid,
        msi.tn_fax_msisdn   as tnv_fax_msisdn,
        msi.tn_fax_status   as tnv_fax_stat,
        msi.tn_fax_valid_to as tnv_fax_valid,
        msi.tc_tel_msisdn   as tc_msisdn,
        msi.tc_tel_status   as tc_ms_stat,
        msi.tc_tel_valid_to as tc_ms_valid,
        msi.tc_dat_msisdn   as tc_dat_msisdn,
        msi.tc_dat_status   as tc_dat_stat,
        msi.tc_dat_valid_to as tc_dat_valid,
        msi.tc_fax_msisdn   as tc_fax_msisdn,
        msi.tc_fax_status   as tc_fax_stat,
        msi.tc_fax_valid_to as tc_fax_valid,
        msi.tb_tel_msisdn   as tb_msisdn,
        msi.tb_tel_status   as tb_ms_stat,
        msi.tb_tel_valid_to as tb_ms_valid,
        msi.tb_dat_msisdn   as tb_dat_msisdn,
        msi.tb_dat_status   as tb_dat_stat,
        msi.tb_dat_valid_to as tb_dat_valid,
        msi.tb_fax_msisdn   as tb_fax_msisdn,
        msi.tb_fax_status   as tb_fax_stat,
        msi.tb_fax_valid_to as tb_fax_valid,
        -- MultiSIM: Feldumbenennungen analog zu da_/vda_/tk_:
        msi.ms_rn_1_msisdn    as ms1_msisdn,   -- 20070111 ME
        msi.ms_rn_1_status    as ms1_ms_stat,
        msi.ms_rn_1_valid_to  as ms1_ms_valid,
        msi.ms_rn_2_msisdn    as ms2_msisdn,
        msi.ms_rn_2_status    as ms2_ms_stat,
        msi.ms_rn_2_valid_to  as ms2_ms_valid,
        msd.da_rn_msisdn    as da_msisdn,
        msd.da_rn_status    as da_ms_stat,
        msd.da_rn_valid_to  as da_ms_valid,
        msd.vda_rn_msisdn   as vda_msisdn,
        msd.vda_rn_status   as vda_ms_stat,
        msd.vda_rn_valid_to as vda_ms_valid,
        msd.tk_rn_msisdn    as tk_msisdn,
        msd.tk_rn_status    as tk_ms_stat,
        msd.tk_rn_valid_to  as tk_ms_valid,
        bccm.cntrct_id_ref    as BCP_VERTRAG,
        bccm.tn_iccid         as BCP_ICCID,
        bccm.tn_imsi_hlr      as BCP_HLR,
        decode(av.apn, null,av.apn,
               av.apn||','||av.apn_cntrct) as  apn, -- This decode needs to be translated to BigQuery CASE WHEN
        bccm.tn_tel_msisdn   as bcp_tn_tel,
        opt.data_option_rein as data_option_rein,
        opt.voice_option_rein     as voice_option_rein,
        opt.mix_option as mix_option,
        opt.multi_option as multi_option,
        opt.roaming_option     as roaming_option,
        opt.sonstige_option as sonstige_option,
        icc.ms3_iccid     as ms3_iccid,                       --Added for multisim3+ 17.2
        icc.ms3_e_id      as ms3_e_id,                        --Added for multisim3+ 17.2
        icc.ms3_card_type_name as ms3_card_type_name,              --Added for multisim3+ 17.2
        icc.ms3_imsi_mcc  as ms3_mcc,                         --Added for multisim3+ 17.2
        icc.ms3_imsi_mnc  as ms3_mnc,                         --Added for multisim3+ 17.2
        icc.ms3_imsi_hlr  as ms3_hlr,                         --Added for multisim3+ 17.2
        icc.ms3_imsi_si   as ms3_si,                          --Added for multisim3+ 17.2
        icc.ms3_status    as ms3_stat,                        --Added for multisim3+ 17.2
        icc.ms3_valid_to  as ms3_valid,                       --Added for multisim3+ 17.2
        icc.ms4_iccid     as ms4_iccid,                       --Added for multisim3+ 17.2
        icc.ms4_e_id      as ms4_e_id,                        --Added for multisim3+ 17.2
        icc.ms4_card_type_name as ms4_card_type_name,              --Added for multisim3+ 17.2
        icc.ms4_imsi_mcc  as ms4_mcc,                         --Added for multisim3+ 17.2
        icc.ms4_imsi_mnc  as ms4_mnc,                         --Added for multisim3+ 17.2
        icc.ms4_imsi_hlr  as ms4_hlr,                         --Added for multisim3+ 17.2
        icc.ms4_imsi_si   as ms4_si,                          --Added for multisim3+ 17.2
        icc.ms4_status    as ms4_stat,                        --Added for multisim3+ 17.2
        icc.ms4_valid_to  as ms4_valid,                       --Added for multisim3+ 17.2
        icc.ms5_iccid     as ms5_iccid,                       --Added for multisim3+ 17.2
        icc.ms5_e_id      as ms5_e_id,                        --Added for multisim3+ 17.2
        icc.ms5_card_type_name as ms5_card_type_name,              --Added for multisim3+ 17.2
        icc.ms5_imsi_mcc  as ms5_mcc,                         --Added for multisim3+ 17.2
        icc.ms5_imsi_mnc  as ms5_mnc,                         --Added for multisim3+ 17.2
        icc.ms5_imsi_hlr  as ms5_hlr,                         --Added for multisim3+ 17.2
        icc.ms5_imsi_si   as ms5_si,                          --Added for multisim3+ 17.2
        icc.ms5_status    as ms5_stat,                        --Added for multisim3+ 17.2
        icc.ms5_valid_to  as ms5_valid,                       --Added for multisim3+ 17.2
        icc.ms6_iccid     as ms6_iccid,                       --Added for multisim3+ 17.2
        icc.ms6_e_id      as ms6_e_id,                        --Added for multisim3+ 17.2
        icc.ms6_card_type_name as ms6_card_type_name,              --Added for multisim3+ 17.2
        icc.ms6_imsi_mcc  as ms6_mcc,                         --Added for multisim3+ 17.2
        icc.ms6_imsi_mnc  as ms6_mnc,                         --Added for multisim3+ 17.2
        icc.ms6_imsi_hlr  as ms6_hlr,                         --Added for multisim3+ 17.2
        icc.ms6_imsi_si   as ms6_si,                          --Added for multisim3+ 17.2
        icc.ms6_status    as ms6_stat,                        --Added for multisim3+ 17.2
        icc.ms6_valid_to  as ms6_valid,                       --Added for multisim3+ 17.2
        icc.ms7_iccid     as ms7_iccid,                       --Added for multisim3+ 17.2
        icc.ms7_e_id      as ms7_e_id,                        --Added for multisim3+ 17.2
        icc.ms7_card_type_name as ms7_card_type_name,              --Added for multisim3+ 17.2
        icc.ms7_imsi_mcc  as ms7_mcc,                         --Added for multisim3+ 17.2
        icc.ms7_imsi_mnc  as ms7_mnc,                         --Added for multisim3+ 17.2
        icc.ms7_imsi_hlr  as ms7_hlr,                         --Added for multisim3+ 17.2
        icc.ms7_imsi_si   as ms7_si,                          --Added for multisim3+ 17.2
        icc.ms7_status    as ms7_stat,                        --Added for multisim3+ 17.2
        icc.ms7_valid_to  as ms7_valid,                       --Added for multisim3+ 17.2
        icc.ms8_iccid     as ms8_iccid,                       --Added for multisim3+ 17.2
        icc.ms8_e_id      as ms8_e_id,                        --Added for multisim3+ 17.2
        icc.ms8_card_type_name as ms8_card_type_name,              --Added for multisim3+ 17.2
        icc.ms8_imsi_mcc  as ms8_mcc,                         --Added for multisim3+ 17.2
        icc.ms8_imsi_mnc  as ms8_mnc,                         --Added for multisim3+ 17.2
        icc.ms8_imsi_hlr  as ms8_hlr,                         --Added for multisim3+ 17.2
        icc.ms8_imsi_si   as ms8_si,                          --Added for multisim3+ 17.2
        icc.ms8_status    as ms8_stat,                        --Added for multisim3+ 17.2
        icc.ms8_valid_to  as ms8_valid,                       --Added for multisim3+ 17.2
        icc.ms9_iccid     as ms9_iccid,                       --Added for multisim3+ 17.2
        icc.ms9_e_id      as ms9_e_id,                        --Added for multisim3+ 17.2
        icc.ms9_card_type_name as ms9_card_type_name,              --Added for multisim3+ 17.2
        icc.ms9_imsi_mcc  as ms9_mcc,                         --Added for multisim3+ 17.2
        icc.ms9_imsi_mnc  as ms9_mnc,                         --Added for multisim3+ 17.2
        icc.ms9_imsi_hlr  as ms9_hlr,                         --Added for multisim3+ 17.2
        icc.ms9_imsi_si   as ms9_si,                          --Added for multisim3+ 17.2
        icc.ms9_status    as ms9_stat,                        --Added for multisim3+ 17.2
        icc.ms9_valid_to  as ms9_valid,                       --Added for multisim3+ 17.2
        icc.ms10_iccid     as ms10_iccid,                      --Added for multisim3+ 17.2
        icc.ms10_e_id      as ms10_e_id,                       --Added for multisim3+ 17.2
        icc.ms10_card_type_name as ms10_card_type_name,             --Added for multisim3+ 17.2
        icc.ms10_imsi_mcc  as ms10_mcc,                        --Added for multisim3+ 17.2
        icc.ms10_imsi_mnc  as ms10_mnc,                        --Added for multisim3+ 17.2
        icc.ms10_imsi_hlr  as ms10_hlr,                        --Added for multisim3+ 17.2
        icc.ms10_imsi_si   as ms10_si,                         --Added for multisim3+ 17.2
        icc.ms10_status    as ms10_stat,                       --Added for multisim3+ 17.2
        icc.ms10_valid_to  as ms10_valid                       --Added for multisim3+ 17.2
  FROM sof$ta_cntrct_dist cn,
       (
       SELECT
              BC.CNTRCT_ID,
              BC.CNTRCT_ID_REF,
              BC.TN_ICCID,
              BC.TN_IMSI_HLR,
              BCM.TN_TEL_MSISDN
         FROM SOF$TA_BCP_ICCID BC,
              SOF$TA_BCP_MSISDN BCM
        WHERE BC.CNTRCT_ID    = BCM.CNTRCT_ID
          AND BC.CNTRCT_ID_REF = BCM.CNTRCT_ID_REF
       ) bccm,
       sof$ta_cntrct_evn      ev,
       sof$ta_iccid_vertrag   icc,
       sof$ta_rn_vertrag      msi,
       sof$ta_rn_da_vda_tk    msd,
       sof$ta_tarifoption     opt,
       sof$ta_apn_vertrag     av
 WHERE cn.cntrct_id = ev.cntrct_id  (+)
   AND cn.cntrct_id = icc.cntrct_id (+)
   AND cn.cntrct_id = msi.cntrct_id (+)
   AND cn.cntrct_id = opt.cntrct_id (+)
   AND cn.cntrct_id = av.cntrct_id  (+)
   AND cn.cntrct_id = msd.cntrct_id (+)\'
   AND cn.cntrct_id = bccm.cntrct_id (+)
;


-- COMMIT; -- Not explicitly needed in BigQuery DML, transactions are handled differently.

-- ========================= Step15 ==================================

-- prompt step15: lschen der temporren zwischentabellen...
---------------------------------------------------------

-- whenever sqlerror continue

-- Some tables left for testing:
--TRUNCATE TABLE sof$ta_msisdn_his REUSE STORAGE;
-- DROP TABLE sof$ta_msisdn;
--TRUNCATE TABLE sof$ta_bpr_basis_his REUSE STORAGE;
-- DROP TABLE sof$ta_bpr_basis;
--TRUNCATE TABLE sof$ta_bpr_instance REUSE STORAGE;
--TRUNCATE TABLE sof$ta_bpr_evn REUSE STORAGE;
--TRUNCATE TABLE sof$ta_bpr_optionen REUSE STORAGE;
--TRUNCATE TABLE sof$ta_bpr_apn REUSE STORAGE;
--TRUNCATE TABLE sof$ta_bpr_beschr REUSE STORAGE;
-- DROP TABLE sof$ta_rn_einzeln;
-- DROP TABLE sof$ta_rn_vertrag;
--TRUNCATE TABLE sof$ta_rn_da_vda_tk REUSE STORAGE;
-- DROP TABLE sof$ta_iccid_einzeln;
-- DROP TABLE sof$ta_iccid_vertrag;
--TRUNCATE TABLE sof$ta_cntrct_dist REUSE STORAGE;
--TRUNCATE TABLE sof$ta_cntrct_evn REUSE STORAGE;
--TRUNCATE TABLE sof$ta_bpr_opt_text REUSE STORAGE;
--TRUNCATE TABLE sof$ta_apn_carmen REUSE STORAGE;
--TRUNCATE TABLE sof$ta_bpr_bcp REUSE STORAGE;
--TRUNCATE TABLE sof$ta_bcp_iccid REUSE STORAGE;
--TRUNCATE TABLE sof$ta_bcp_msisdn REUSE STORAGE;
-- These TRUNCATE/DROP statements would need to be translated to BigQuery syntax if they are active requirements.
-- In BigQuery, tables are often not dropped/truncated directly but overwritten or partitioned.


-- ************************* Step16 **********************************

-- prompt step16: Verarbeitung von 'd_ausd_bp_ta_p_basisprod (d_ausd_basisprodukt.sql)' fehlerfrei beendet.
-----------------------------------------------------------------------------

-- spool off
-- exit success