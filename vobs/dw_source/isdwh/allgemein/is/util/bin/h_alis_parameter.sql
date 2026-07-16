-- =====================================================================
-- Target File: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parameter.sql
-- Description: Parameter parsing, validation, and conversion routines.
-- =====================================================================

-- =====================================================================
-- UDF: konvertiereKennzahl_lookup
-- Description: Pure function encapsulating the business mapping logic
--              for metric codes. Returns '???' if not recognized.
-- =====================================================================
CREATE OR REPLACE FUNCTION `@GCP_PROJECT.@BQ_DATASET.konvertiereKennzahl_lookup`(Kennzahl STRING)
RETURNS STRING AS (
  CASE LOWER(TRIM(Kennzahl))
    WHEN 'abgang' THEN 'abg'
    WHEN 'artikel' THEN 'artikel'
    WHEN 'abgang_zukunft' THEN 'abz'
    WHEN 'aktivierung' THEN 'akq'
    WHEN 'aktivitaet_id' THEN 'akti'
    WHEN 'aktivitaet_summe' THEN 'akti_sum'
    WHEN 'anz_u18_personen' THEN 'persu18'
    WHEN 'antwort' THEN 'antwor'
    WHEN 'apn' THEN 'apn'
    WHEN 'aufladung' THEN 'auf'
    WHEN 'basisprodukt_abg' THEN 'bpr_abg'
    WHEN 'basisprodukt_zug' THEN 'bpr_zug'
    WHEN 'basisprodukt_zugabg' THEN 'bpi'
    WHEN 'basisdienst' THEN 'basisd'
    WHEN 'bds_vo_kenn' THEN 'bds_vo'
    WHEN 'bearbeitung' THEN 'ngbearb'
    WHEN 'bestand' THEN 'bst'
    WHEN 'bestand_virtuell' THEN 'bst_v'
    WHEN 'bestellweg' THEN 'lor'
    WHEN 'bewegart' THEN 'bwa'
    WHEN 'brutto_abgang' THEN 'babg'
    WHEN 'brutto_zugang' THEN 'bzug'
    WHEN 'bpi_zugang_kond' THEN 'bpi_zug_kond'
    WHEN 'bpi_abgang_kond' THEN 'bpi_abg_kond'
    WHEN 'bundesland' THEN 'geo_bl'
    WHEN 'carmen_gutschrift_sap' THEN 'cgs'
    WHEN 'carmen_rechnung_sap' THEN 'crs'
    WHEN 'carmen_rechnung_sap_koepfe' THEN 'crsk'
    WHEN 'carmen_rechnung_sap_budgets' THEN 'crsb'
    WHEN 'carmen_rechnung_sap_rabatt' THEN 'crsr'
    WHEN 'carmen_rechnung_sap_xkopf' THEN 'crsxk'
    WHEN 'carmen_rechnung_sap_xtra' THEN 'crsx'
    WHEN 'etg_verzehr' THEN 'crs_etg'
    WHEN 'cellid_region_mapping' THEN 'cell_map'
    WHEN 'd1news' THEN 'd1n'
    WHEN 'dolphin_vorprodukte' THEN 'map_vprod'
    WHEN 'dpps_gutschrift_sap' THEN 'dgs'
    WHEN 'dpps_rechnung_sap' THEN 'drs'
    WHEN 'dwh_alterssegment' THEN 'alter_sgmnt'
    WHEN 'einmalige_guthaben' THEN 'etg'
    WHEN 'ees_ereignis_log' THEN 'ese'
    WHEN 'eva_gp' THEN 'eva_gp'
    WHEN 'eva_rd' THEN 'eva_rd'
    WHEN 'eva_rv' THEN 'eva_rv'
    WHEN 'eva_vt' THEN 'eva_vt'
    WHEN 'fakturierung_fact' THEN 'fakt_fac'
    WHEN 'feiertag' THEN 'ft'
    WHEN 'frage' THEN 'frage'
    WHEN 'gemeinde' THEN 'geo_gmd'
    WHEN 'geschaeftsprozesse' THEN 'gproz'
    WHEN 'gespraechslaengenverteilung' THEN 'glv'
    WHEN 'gespraechsvolumenverteilung_mms' THEN 'gvv_mms'
    WHEN 'gespraechsvolumenverteilung_gprs' THEN 'gvv_gprs'
    WHEN 'netznutzung_gprs' THEN 'nnv_gprs'
    WHEN 'netznutzung_budget' THEN 'nnv_budge'
    WHEN 'basisprodukt_budget' THEN 'bpr_budge'
    WHEN 'taegliche_budgetausnutzung' THEN 'budge_gza'
    WHEN 'gespraechstyp' THEN 'gtyp'
    WHEN 'gespraechsziele' THEN 'gz'
    WHEN 'glaengenintervall' THEN 'glint'
    WHEN 'gprs' THEN 'gprs'
    WHEN 'gr_nnv_tvd_leist' THEN 'gr_nnvtvd'
    WHEN 'gr_rechpos_leist' THEN 'gr_rpos'
    WHEN 'rechnungen_detail' THEN 'rpos_det'
    WHEN 'gutschrift' THEN 'gut'
    WHEN 'gutschrift_rv' THEN 'sg_rv'
    WHEN 'ilv_ausnahmen' THEN 'ilv_ausn'
    WHEN 'indiv_festnetzzahlen' THEN 'idv_fa'
    WHEN 'initztsf' THEN 'initztsf'
    WHEN 'initztss' THEN 'initztss'
    WHEN 'ip_deb_schluessel' THEN 'ipdebs'
    WHEN 'ip_debitor' THEN 'ipdeb'
    WHEN 'itc_verkehrsmengen' THEN 'itc_fa'
    WHEN 'kampagnensegment' THEN 'kamp_seg'
    WHEN 'kampagne' THEN 'kamp'
    WHEN 'karte' THEN 'kart'
    WHEN 'kategorie' THEN 'lsc'
    WHEN 'kes_autogv' THEN 'kes_autogv'
    WHEN 'korrvertrag' THEN 'vtg'
    WHEN 'korr_hist_dpps' THEN 'hist'
    WHEN 'kostenstelle' THEN 'kstl'
    WHEN 'kreis' THEN 'geo_krs'
    WHEN 'kundenstamm' THEN 'ksd'
    WHEN 'kundenwertprogramm' THEN 'tkwpt'
    WHEN 'kundenwertprogrammpunkte' THEN 'tkwpp'
    WHEN 'leistungsklasse' THEN 'lkl'
    WHEN 'liefermodus' THEN 'lmo'
    WHEN 'loeschung' THEN 'loe'
    WHEN 'mahnstufe' THEN 'mahn'
    WHEN 'map_leistungsklasse' THEN 'map_lk'
    WHEN 'map_basisprodukt_budget' THEN 'bpr_budget'
    WHEN 'mapping_vas_contentyp' THEN 'vas_cont_ty'
    WHEN 'mapping_zelle_region' THEN 'map_zelle'
    WHEN 'mapping_apn_typ' THEN 'map_apn'
    WHEN 'mapping_mcc_mnc' THEN 'mccmnc'
    WHEN 'metadatenstruktur' THEN 'mds'
    WHEN 'mms_volumenklassen' THEN 'gvvk'
    WHEN 'mms_volumenklassen_gruppen' THEN 'gvvkgr'
    WHEN 'mms_quellen' THEN 'mmsq'
    WHEN 'mms_richtungen' THEN 'mmsr'
    WHEN 'mms_ziele' THEN 'mmsz'
    WHEN 'mms_zonen_typ' THEN 'mms_zonet'
    WHEN 'morpu_bpr_monerloes' THEN 'morpu_bpr'
    WHEN 'morpu_id' THEN 'morpu'
    WHEN 'morpu_map_tvd' THEN 'morpu_tvd'
    WHEN 'morpu_map_lid' THEN 'morpu_lid'
    WHEN 'morpu_map_quelle' THEN 'morpu_quell'
    WHEN 'morpu_map_attraktoren' THEN 'morpu_attr'
    WHEN 'morpu_factoring_parameter' THEN 'morpu_param'
    WHEN 'morpu_map_lid_gru' THEN 'morpu_gru'
    WHEN 'morpu_map_lk_mtc' THEN 'morpu_mtc'
    WHEN 'morpu_map_preis' THEN 'morpu_preis'
    WHEN 'morpu_map_anzahl' THEN 'morpu_anzal'
    WHEN 'morpu_map_cwb_produkttext' THEN 'morpu_cwb_p'
    WHEN 'nationalinternational' THEN 'natint'
    WHEN 'netznutzungsklassen' THEN 'nnk'
    WHEN 'netznutzungsklassentyp' THEN 'nnkt'
    WHEN 'netznutzung_reselling' THEN 'reselling'
    WHEN 'netznutzung_fmn' THEN 'nnv_fmn'
    WHEN 'netznutzung_mms' THEN 'nnv_mms'
    WHEN 'zellen_nutzung' THEN 'nnv_zelle'
    WHEN 'ng_auftraege_fehler' THEN 'ngfehlauf'
    WHEN 'ng_aktivierung_es' THEN 'ngakq_es'
    WHEN 'ng_aktivitaeten' THEN 'ngaktiv'
    WHEN 'ng_fehler' THEN 'ngfehl'
    WHEN 'ng_fehlerrueck' THEN 'ngrueck'
    WHEN 'ng_rueckst' THEN 'ngrueckst'
    WHEN 'ng_vorgang' THEN 'ngvorgang'
    WHEN 'ng_vorgang_es' THEN 'ngvd_es'
    WHEN 'ng_wna_dlz' THEN 'ngwnadlz'
    WHEN 'ng_zielmanagement_k4' THEN 'ngzm_k4'
    WHEN 'opal' THEN 'opal'
    WHEN 'paket' THEN 'paket'
    WHEN 'performance' THEN 'perf'
    WHEN 'plan' THEN 'pln'
    WHEN 'pos' THEN 'pos'
    WHEN 'fakturierung' THEN 'fact'
    WHEN 'ratingreloaded_budgets' THEN 'budgets'
    WHEN 'probiss_forderungen' THEN 'prob_ford'
    WHEN 'probiss_gutschriften' THEN 'prob_guts'
    WHEN 'pri' THEN 'pri'
    WHEN 'preisstufen_fakturierung' THEN 'preis_fac'
    WHEN 'produkt' THEN 'produ'
    WHEN 'punkteart' THEN 'pnktart'
    WHEN 'punkteursprung' THEN 'pktu'
    WHEN 'punktezugang_detail' THEN 'pnktzgd'
    WHEN 'punkte_abg_ges' THEN 'pnkt_ab'
    WHEN 'punkte_zug_ges' THEN 'pnkt_zg'
    WHEN 'reaktivierung' THEN 'rak'
    WHEN 'rechnungen_rv_dpps' THEN 'sr_rv_dpps'
    WHEN 'regierungsbez' THEN 'geo_rgb'
    WHEN 'repprodmatrix' THEN 'rep_x'
    WHEN 'restguthaben' THEN 'rst'
    WHEN 'risc' THEN 'ngrisc'
    WHEN 'rqtvarch' THEN 'rqtvarch'
    WHEN 'rubrik' THEN 'rub'
    WHEN 'rv_imei' THEN 'rv_imei'
    WHEN 'scheck' THEN 'scheck'
    WHEN 'sia_measures_fc' THEN 'sia_mea_fc'
    WHEN 'sia_measures_qs' THEN 'sia_mea_qs'
    WHEN 'spcap_whs' THEN 'spcap_whs'
    WHEN 'stab' THEN 'geo_stb'
    WHEN 'standard_gutschrift' THEN 'sgs'
    WHEN 'standard_rechnung' THEN 'srs'
    WHEN 'strasse_absch' THEN 'geo_str'
    WHEN 'tagesnutzungsdaten' THEN 'tnd'
    WHEN 'tagesverkehrskurven' THEN 'tvk'
    WHEN 'tarifart' THEN 'trfa'
    WHEN 'tarifvariante' THEN 'tarif_var'
    WHEN 'tarifwechsel' THEN 'twe'
    WHEN 'teilnehmer' THEN 'tln_sd'
    WHEN 'teilnehmerverbindungsdaten' THEN 'tvd'
    WHEN 'teilnehmer_ds' THEN 'tln_ds'
    WHEN 'umts' THEN 'umts'
    WHEN 'uskonto' THEN 'usk'
    WHEN 'usteilnehmer' THEN 'ust'
    WHEN 'vertragsverlaengerung' THEN 'vbd'
    WHEN 've_all_storno_zpkt_z' THEN 've_all_b'
    WHEN 've_basisprodukt_abgang' THEN 've_bp_a'
    WHEN 've_basisprodukt_zugang' THEN 've_bp_z'
    WHEN 've_basisprodukt_rvzv_abgang' THEN 've_bprzv_a'
    WHEN 've_basisprodukt_rvzv_zugang' THEN 've_bprzv_z'
    WHEN 've_bp_storno_zpkt_z' THEN 've_bp_b'
    WHEN 've_bp_rvzv_storno_zpkt_z' THEN 've_bprzv_b'
    WHEN 've_ees_n1_whlg' THEN 've_ees_n1_whlg'
    WHEN 've_ees_n3_whlg' THEN 've_ees_n3_whlg'
    WHEN 've_ees_s0_init' THEN 've_ees_s0_init'
    WHEN 've_ees_s1_zpkt' THEN 've_ees_s1_zpkt'
    WHEN 've_ees_s2_aufg' THEN 've_ees_s2_aufg'
    WHEN 've_ees_s3_kont' THEN 've_ees_s3_kont'
    WHEN 've_ees_s4_kamp' THEN 've_ees_s4_kamp'
    WHEN 've_ees_s5_merge' THEN 've_ees_s5_merge'
    WHEN 've_neuvertrag_zugang' THEN 've_nv_z'
    WHEN 've_neuvertrag_abgang' THEN 've_nv_a'
    WHEN 've_nv_storno_zpkt_z' THEN 've_nv_b'
    WHEN 've_twe_c2c_zugang' THEN 've_c2c_z'
    WHEN 've_twe_c2c_abgang' THEN 've_c2c_a'
    WHEN 've_twe_c2c_storno_zpkt_z' THEN 've_c2c_b'
    WHEN 've_twe_x2c_zugang' THEN 've_x2c_z'
    WHEN 've_twe_x2c_abgang' THEN 've_x2c_a'
    WHEN 've_twe_x2c_storno_zpkt_z' THEN 've_x2c_b'
    WHEN 've_vvl_zugang' THEN 've_vvl_z'
    WHEN 've_vvl_abgang' THEN 've_vvl_a'
    WHEN 've_vvl_storno_zpkt_z' THEN 've_vvl_b'
    WHEN 've_vvlsp_zugang' THEN 've_vvlsp_z'
    WHEN 've_vvlsp_abgang' THEN 've_vvlsp_a'
    WHEN 've_vvlsp_storno_zpkt_z' THEN 've_vvlsp_b'
    WHEN 've_vvltm_zugang' THEN 've_vvltm_z'
    WHEN 've_vvltm_abgang' THEN 've_vvltm_a'
    WHEN 've_vvltm_storno_zpkt_z' THEN 've_vvltm_b'
    WHEN 'volumenklassen' THEN 'volklasse'
    WHEN 'vorgang' THEN 'vorg'
    WHEN 'vorgang2' THEN 'vorg2'
    WHEN 'vo_regionalstruktur' THEN 'plz_region'
    WHEN 'vertragsverlaengerung_tarifwechsel_ereignisse' THEN 'twvv_e'
    WHEN 'vertragsverlaengerung_tarifwechsel' THEN 'twvv_gv'
    WHEN 'vertragsverlaengerung_tarifwechsel_ereignisse_oo' THEN 'twvv_e_oo'
    WHEN 'vvl' THEN 'vvl'
    WHEN 'xtra_auszahlungen_sap' THEN 'xtra_verfal'
    WHEN 'wap' THEN 'wap'
    WHEN 'wna_smd' THEN 'wna_smd'
    WHEN 'zonenkennung' THEN 'zonek'
    WHEN 'zonentyp' THEN 'zonet'
    WHEN 'zeitzonen' THEN 'zeitz'
    WHEN 'zugang' THEN 'zug'
    WHEN 'nutzungshaeufigkeit' THEN 'nutz_haeuf'
    WHEN 'ivr_brutto_zugang' THEN 'ivr_bzug'
    WHEN 'ivr_brutto_abgang' THEN 'ivr_babg'
    WHEN 'ivr_tarifwechsel' THEN 'twe_ivr'
    WHEN 'gdp_nutz' THEN 'gdp_nutz'
    ELSE '???'
  END
);

-- =====================================================================
-- UDF: konvertiereSDName_lookup
-- Description: Lookup engine for master data mappings.
-- =====================================================================
CREATE OR REPLACE FUNCTION `@GCP_PROJECT.@BQ_DATASET.konvertiereSDName_lookup`(System STRING)
RETURNS STRING AS (
  CASE LOWER(TRIM(System))
    WHEN 'aufladung' THEN 'auf'
    WHEN 'basisprodukt_abg' THEN 'carmen'
    WHEN 'basisprodukt_zug' THEN 'carmen'
    WHEN 'basisprodukt_zugabg' THEN 'carmen'
    WHEN 'bewegart' THEN 'bwa'
    WHEN 'cash_partner' THEN 'cap'
    WHEN 'distributor' THEN 'dist'
    WHEN 'ergebnis_dc' THEN 'ergdc'
    WHEN 'frachtfuehrer' THEN 'frfu'
    WHEN 'gutschrift' THEN 'gut'
    WHEN 'gutschrift_grund' THEN 'l_gutgr'
    WHEN 'indiv_auf_produkt' THEN 'idv_pt'
    WHEN 'indiv_auf_zugangsart' THEN 'idv_za'
    WHEN 'indiv_gesch_vorfall' THEN 'idv_gv'
    WHEN 'indiv_gesch_vorfall_dsl' THEN 'idv_gd'
    WHEN 'indiv_pos_produkt' THEN 'idv_pp'
    WHEN 'indiv_praemie' THEN 'idv_pr'
    WHEN 'indiv_praemie_schalter' THEN 'idv_ps'
    WHEN 'indiv_steuerung' THEN 'idv_sg'
    WHEN 'indiv_steuerung_tonline' THEN 'idv_st'
    WHEN 'itc_aggregation_type' THEN 'itc_at'
    WHEN 'itc_entf_zonen' THEN 'itc_zo'
    WHEN 'itc_entf_zonen_gruppen' THEN 'itc_zg'
    WHEN 'itc_tarife' THEN 'itc_tf'
    WHEN 'itc_verkehrsrichtung' THEN 'itc_vr'
    WHEN 'itc_waehrung_sdr' THEN 'itc_sdr'
    WHEN 'kdg_grund' THEN 'kdg'
    WHEN 'landkode' THEN 'lkode'
    WHEN 'leistung' THEN 'l_leist'
    WHEN 'mahnstufentyp_sapist' THEN 'l_mahnstyp_ist'
    WHEN 'mahnverfahren_sapfi' THEN 'l_mahnv_fi'
    WHEN 'mahnverfahren_sapist' THEN 'l_mahnv_ist'
    WHEN 'opal' THEN 'sap'
    WHEN 'postkorb' THEN 'postko'
    WHEN 'produkt' THEN 'l_prod'
    WHEN 'rahmenvertrag' THEN 'rv'
    WHEN 'reklart' THEN 'reklart'
    WHEN 'reklentscheidung' THEN 'reklent'
    WHEN 'reklgrund' THEN 'reklgr'
    WHEN 'reklprodukt' THEN 'reklpr'
    WHEN 'rekltyp' THEN 'rekltyp'
    WHEN 'reklursache' THEN 'reklurs'
    WHEN 'rv_aktionskennzeichen' THEN 'rvakz'
    WHEN 'sap_gutschrift_grund' THEN 'sap_l_gutgr'
    WHEN 'sonderkarten' THEN 'sk'
    WHEN 'status' THEN 'status'
    WHEN 'tarif' THEN 'trf'
    WHEN 'tibco' THEN 'tibco'
    WHEN 'acl_omsoe' THEN 'acl_omsoe'
    WHEN 'tstatus' THEN 'ts'
    WHEN 'vo' THEN 'vo'
    WHEN 'wapkat' THEN 'wapkat'
    WHEN 'zahlmodus' THEN 'zm'
    WHEN 'vobetr' THEN 'vobetr'
    WHEN 'vokam' THEN 'vokam'
    WHEN 'ad_betreu' THEN 'ad_betreu'
    WHEN 'rqtvarch' THEN 'dwh'
    ELSE '???'
  END
);

-- ---------------------------------------------------------------------
-- Procedure: pruefeParameterGesetzt
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@GCP_PROJECT.@BQ_DATASET.pruefeParameterGesetzt`(
  IN param_name STRING,
  IN param_wert STRING,
  INOUT ErrNr INT64,
  INOUT ErrArg STRING
)
BEGIN
  IF ErrNr = 0 THEN
    IF param_name IS NULL OR param_name = '' THEN
      SET ErrNr = 196;
      SET ErrArg = 'alis_parameter V8.3.1 pruefeParameterGesetzt';
    ELSEIF param_wert IS NULL OR param_wert = '' THEN
      SET ErrNr = 194;
      SET ErrArg = param_name;
    END IF;
  END IF;
END;

-- ---------------------------------------------------------------------
-- Procedure: konvertiereKennzahl
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@GCP_PROJECT.@BQ_DATASET.konvertiereKennzahl`(
  INOUT Kennzahl STRING,
  INOUT ErrNr INT64,
  INOUT ErrArg STRING
)
BEGIN
  DECLARE mapped_val STRING;
  IF ErrNr = 0 THEN
    IF Kennzahl IS NULL OR Kennzahl = '' THEN
      SET ErrNr = 196;
      SET ErrArg = 'alis_parameter V8.3.1 konvertiereKennzahl';
    ELSE
      SET mapped_val = `@GCP_PROJECT.@BQ_DATASET.konvertiereKennzahl_lookup`(Kennzahl);
      IF mapped_val = '???' THEN
        SET ErrNr = 198;
        SET ErrArg = LOWER(TRIM(Kennzahl));
      ELSE
        SET Kennzahl = mapped_val;
      END IF;
    END IF;
  END IF;
END;

-- ---------------------------------------------------------------------
-- Procedure: konvertiereSystem
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@GCP_PROJECT.@BQ_DATASET.konvertiereSystem`(
  INOUT System STRING,
  INOUT ErrNr INT64,
  INOUT ErrArg STRING
)
BEGIN
  DECLARE normalized_val STRING;
  IF ErrNr = 0 THEN
    IF System IS NULL OR System = '' THEN
      SET ErrNr = 196;
      SET ErrArg = 'alis_parameter V8.3.1 konvertiereSystem';
    ELSE
      SET normalized_val = LOWER(TRIM(System));
      IF normalized_val IN (
        'bapsi', 'brunet', 'cap_dwh', 'carmen', 'ctel', 'd1', 'dpps', 'dwh',
        'gateway', 'indiv', 'kkm', 'kws', 'nnv', 'planf2', 'rr', 'sap', 'sd',
        'sigma', 'tibco', 'acl_omsoe', 'vo', 'vpquick', 'xtra', 'zts'
      ) THEN
        SET System = normalized_val;
      ELSE
        SET ErrNr = 195;
        SET ErrArg = CONCAT('Unbekannte Datenherkunft ', System, ' !');
        SET System = '???';
      END IF;
    END IF;
  END IF;
END;

-- ---------------------------------------------------------------------
-- Procedure: konvertiereSDName
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@GCP_PROJECT.@BQ_DATASET.konvertiereSDName`(
  INOUT System STRING,
  INOUT ErrNr INT64,
  INOUT ErrArg STRING
)
BEGIN
  DECLARE mapped_val STRING;
  IF ErrNr = 0 THEN
    IF System IS NULL OR System = '' THEN
      SET ErrNr = 196;
      SET ErrArg = 'alis_parameter V8.3.1 konvertiereSDSystem';
    ELSE
      SET mapped_val = `@GCP_PROJECT.@BQ_DATASET.konvertiereSDName_lookup`(System);
      IF mapped_val = '???' THEN
        SET ErrNr = 195;
        SET ErrArg = CONCAT('Unbekannte Stammdaten-Datenherkunft ', LOWER(TRIM(System)), ' !');
      ELSE
        SET System = mapped_val;
      END IF;
    END IF;
  END IF;
END;

-- ---------------------------------------------------------------------
-- Procedure: konvertiereAufbStufeXtra
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@GCP_PROJECT.@BQ_DATASET.konvertiereAufbStufeXtra`(
  INOUT Stufe STRING,
  INOUT ErrNr INT64,
  INOUT ErrArg STRING
)
BEGIN
  DECLARE normalized_val STRING;
  IF ErrNr = 0 THEN
    IF Stufe IS NULL OR Stufe = '' THEN
      SET ErrNr = 196;
      SET ErrArg = 'alis_parameter V8.3.1 konvertiereAufbStufeXtra';
    ELSE
      SET normalized_val = LOWER(TRIM(Stufe));
      IF normalized_val = 'befuellung' THEN
        SET Stufe = 'fill';
      ELSEIF normalized_val = 'zusammenfuehrung' THEN
        SET Stufe = 'mrg';
      ELSE
        SET ErrNr = 195;
        SET ErrArg = CONCAT('Unbekannte Stufenangabe ', Stufe, ' !');
        SET Stufe = '???';
      END IF;
    END IF;
  END IF;
END;

-- ---------------------------------------------------------------------
-- Procedure: pruefeSystemKennzahl
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@GCP_PROJECT.@BQ_DATASET.pruefeSystemKennzahl`(
  IN System STRING,
  IN Kennzahl STRING,
  INOUT ErrNr INT64,
  INOUT ErrArg STRING
)
BEGIN
  IF ErrNr = 0 THEN
    IF System IS NULL OR System = '' OR Kennzahl IS NULL OR Kennzahl = '' THEN
      SET ErrNr = 196;
      SET ErrArg = 'alis_parameter V8.3.1 pruefeSystemKennzahl';
    ELSE
      SET ErrArg = '';
      
      IF System = 'bapsi' THEN
        IF Kennzahl != 'itc_fa' THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      ELSEIF System = 'brunet' THEN
        IF Kennzahl NOT IN ('d1n', 'rub', 'lmo', 'lsc', 'lor') THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      ELSEIF System = 'carmen' THEN
        IF Kennzahl IN ('pln', 'rst', 'srs', 'sgs', 'ust', 'mahn', 'sg_rv', 'sr_rv_dpps', 'bwa', 'gproz', 'tkwpt', 'tkwpp', 'cap', 'twvv_gv', 'twvv_e_oo') THEN
          SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
        END IF;
      ELSEIF System = 'ctel' THEN
        IF Kennzahl NOT IN ('abg', 'bst', 'zug', 'twe') THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      ELSEIF System = 'd1' THEN
        IF Kennzahl IN ('gut', 'auf', 'loe', 'rak', 'sgs', 'srs', 'twe', 'ksd', 'mahn', 'sg_rv', 'sr_rv_dpps', 'bwa', 'gproz', 'tkwpt', 'tkwpp', 'akq', 'twvv_e', 'twvv_e_oo') THEN
          SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
        END IF;
      ELSEIF System = 'dpps' THEN
        IF Kennzahl IN ('twe', 'pln', 'loe', 'rak', 'srs', 'sgs', 'mahn', 'sg_rv', 'sr_rv_dpps', 'gproz', 'tkwpt', 'tkwpp', 'akq', 'twvv_gv', 'twvv_e', 'twvv_e_oo') THEN
          SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
        END IF;
      ELSEIF System = 'dwh' THEN
        IF Kennzahl NOT IN (
          'mds', 'gproz', 'bds_vo', 'akti_sum', 'akti', 'map_lk', 'kamp_seg', 'morpu',
          'morpu_tvd', 'morpu_lid', 'morpu_quell', 'morpu_cwb_p', 'morpu_attr', 'morpu_param',
          'morpu_anzal', 'gr_rpos', 'gr_nnvtvd', 'morpu_bpr', 'cell_map', 'map_apn', 'mccmnc',
          'morpu_gru', 'morpu_mtc', 'morpu_preis', 'bpr_budget', 'rqtvarch', 'map_vprod',
          'alter_sgmnt', 'nutz_haeuf', 'vas_cont_ty', 'twvv_e_oo'
        ) THEN
          SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
        END IF;
      ELSEIF System = 'gateway' THEN
        IF Kennzahl != 'wap' THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      ELSEIF System = 'indiv' THEN
        IF Kennzahl != 'idv_fa' THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      ELSEIF System = 'kws' THEN
        IF Kennzahl NOT IN ('eva_gp', 'eva_rd', 'eva_rv', 'eva_vt') THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      ELSEIF System = 'nnv' THEN
        IF Kennzahl NOT IN ('tvd', 'lkl') THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      ELSEIF System = 'planf2' THEN
        IF Kennzahl NOT IN ('bst', 'zug', 'abg') THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      ELSEIF System = 'sap' THEN
        IF Kennzahl IN ('zug', 'abg', 'abz', 'bst', 'twe', 'pln', 'gut', 'auf', 'rst', 'tvd', 'usk', 'ust', 'lkl', 'loe', 'rak', 'ksd', 'bwa', 'gproz', 'akq', 'twvv_gv', 'twvv_e', 'twvv_e_oo') THEN
          SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
        END IF;
      ELSEIF System != 'sap' THEN
        IF Kennzahl IN ('crs', 'cgs', 'drs', 'dgs', 'opal', 'twvv_gv', 'twvv_e', 'rpos_carm', 'crs_etg', 'xtra_verfal', 'crsk', 'crsb', 'rep_x', 'crsx', 'crsxk', 'crsr') THEN
          SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
        END IF;
      ELSEIF System = 'sigma' THEN
        IF Kennzahl NOT IN (
          'gprs', 'nnk', 'tvk', 'glv', 'gz', 'zonek', 'zonet', 'nnkt', 'trfa', 'gtyp',
          'basisd', 'natint', 'glint', 'tnd', 'zeitz', 'reselling', 'prob_ford', 'prob_fact',
          'fact', 'nnv_fmn', 'preis_fac', 'fakt_fac', 'nnv_mms', 'gvv_mms', 'gvvk', 'gvvkgr',
          'mmsz', 'mmsq', 'mmsr', 'nnv_zelle', 'map_zelle', 'tarif_var', 'nnv_gprs', 'gvv_gprs',
          'volklasse', 'nnv_budge', 'bpr_budge', 'mms_zonet', 'budge_gza', 'spcap_whs', 'gdp_nutz', 'rv_imei'
        ) THEN
          SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
        END IF;
      ELSEIF System = 'tibco' THEN
        IF Kennzahl NOT IN ('pos', 'vvl') THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      ELSEIF System = 'acl_omsoe' THEN
        IF Kennzahl NOT IN ('pos', 'vvl') THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      ELSEIF System = 'vpquick' THEN
        IF Kennzahl NOT IN ('perf', 'vorg', 'ft', 'vorg2', 'artikel') THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      ELSEIF System = 'xtra' THEN
        IF Kennzahl != 'rst' THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      ELSEIF System = 'vo' THEN
        IF Kennzahl != 'plz_region' THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      ELSEIF System = 'rr' THEN
        IF Kennzahl != 'budgets' THEN SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl); END IF;
      END IF;

      IF ErrArg != '' THEN
        SET ErrNr = 195;
      END IF;
    END IF;
  END IF;
END;

-- ---------------------------------------------------------------------
-- Procedure: gibBereich
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@GCP_PROJECT.@BQ_DATASET.gibBereich`(
  IN Kennzahl STRING,
  OUT VarBereich STRING,
  INOUT ErrNr INT64,
  INOUT ErrArg STRING
)
BEGIN
  IF ErrNr = 0 THEN
    IF Kennzahl IS NULL OR Kennzahl = '' OR VarBereich IS NULL THEN
      SET ErrNr = 196;
      SET ErrArg = 'alis_parameter V8.3.1 gibBereich';
    ELSE
      SET VarBereich = CASE
        WHEN Kennzahl IN ('basisd', 'd1n', 'glint', 'glv', 'gtyp', 'gz', 'lkl', 'lmo', 'lor', 'lsc', 'tarif_var', 'natint', 'nnk', 'nnkt', 'reselling', 'rub', 'prob_guts', 'prob_ford', 'preis_fac', 'fakt_fac', 'fact', 'nnv_mms', 'gvv_mms', 'gvvk', 'gvvkgr', 'mmsz', 'mmsq', 'mmsr', 'nnv_fmn', 'nnv_zelle', 'map_zelle', 'spcap_whs', 'trfa', 'tvd', 'tvk', 'gvv_gprs', 'nnv_gprs', 'volklasse', 'nnv_budge', 'bpr_budge', 'budge_gza', 'wap', 'zeitz', 'zonek', 'zonet', 'gdp_nutz', 'rv_imei') THEN 'gd'
        WHEN Kennzahl IN ('eva_gp', 'eva_rd', 'eva_rv', 'eva_vt') THEN 'kw'
        WHEN Kennzahl = 'mds' THEN 'md'
        WHEN Kennzahl IN ('kes_autogv', 'ilv_ausn') THEN 'pz'
        WHEN Kennzahl IN ('ft', 'perf', 'vorg', 'vorg2', 'artikel') THEN 'rk'
        WHEN Kennzahl IN ('antwor', 'bds_vo', 'bwa', 'cap', 'frage', 'geo_bl', 'geo_gmd', 'geo_krs', 'geo_rgb', 'geo_stb', 'geo_str', 'gproz', 'gr_rpos', 'gr_nnvtvd', 'kamp', 'hist', 'ipdebs', 'ipdeb', 'kamp_seg', 'kart', 'ksd', 'kstl', 'morpu', 'morpu_bpr', 'morpu_tvd', 'morpu_lid', 'morpu_quell', 'map_apn', 'mccmnc', 'morpu_attr', 'morpu_param', 'morpu_gru', 'morpu_mtc', 'morpu_preis', 'morpu_anzal', 'morpu_cwb_p', 'persu18', 'pktu', 'pnktart', 'produ', 'bpr_budget', 'map_vprod', 'tln_ds', 'tln_sd', 'plz_region', 'alter_sgmnt', 'nutz_haeuf', 'mms_zonet', 'vas_cont_ty', 'vtg', 'cell_map', 'ese', 'rep_x') THEN 'sd'
        WHEN Kennzahl IN ('abg', 'abz', 'akq', 'akti', 'akti_sum', 'apn', 'ngbearb', 'ngrisc', 'rqtvarch', 'bpr_abg', 'bpr_zug', 'bst', 'bpi', 'etg', 'gprs', 'idv_fa', 'loe', 'map_lk', 'pln', 'pri', 'rak', 'tkwpt', 'tnd', 'twe', 'twe_ivr', 'vbd', 've_all_b', 've_bp_z', 've_bp_a', 've_bp_b', 've_bprzv_z', 've_bprzv_a', 've_bprzv_b', 've_c2c_z', 've_c2c_a', 've_c2c_b', 've_ees_n1_whlg', 've_ees_n3_whlg', 've_ees_s0_init', 've_ees_s1_zpkt', 've_ees_s2_aufg', 've_ees_s3_kont', 've_ees_s4_kamp', 've_ees_s5_merge', 've_nv_z', 've_nv_a', 've_nv_b', 've_vvl_z', 've_vvl_a', 've_vvl_b', 've_vvlsp_z', 've_vvlsp_a', 've_vvlsp_b', 've_vvltm_z', 've_vvltm_a', 've_vvltm_b', 've_x2c_z', 've_x2c_a', 've_x2c_b', 'zug', 'bzug', 'babg', 'bpi_abg', 'api_zug', 'ivr_bzug', 'ivr_babg', 'twvv_gv', 'twvv_e', 'twvv_e_oo', 'vvl', 'pos') THEN 'tn'
        WHEN Kennzahl IN ('auf', 'budgets', 'cgs', 'crs', 'crsk', 'crsb', 'crsr', 'rpos_carm', 'crs_etg', 'crsx', 'crsxk', 'dgs', 'drs', 'gut', 'itc_fa', 'initztsf', 'initztss', 'mahn', 'opal', 'paket', 'pnkt_ab', 'pnkt_zg', 'pnktzgd', 'rst', 'rpos_det', 'scheck', 'sg_rv', 'sr_rv_dpps', 'srs', 'sgs', 'tkwpp', 'ust', 'usk', 'xtra_verfal') THEN 'us'
        WHEN Kennzahl IN ('ngakq_es', 'ngaktiv', 'ngvd_es', 'ngvorgang', 'ngzm_k4', 'ngrueckst', 'ngfehl', 'ngrueck', 'ngfehlauf', 'ngwnadlz', 'wna_smd') THEN 'vg'
        WHEN Kennzahl IN ('sia_measures_fc', 'sia_measures_qs') THEN 'ia'
        ELSE NULL
      END;

      IF VarBereich IS NULL THEN
        SET ErrNr = 196;
        SET ErrArg = CONCAT('alis_parameter V8.3.1 gibBereich - Kuerzel ', Kennzahl, ' unbekannt');
      END IF;
    END IF;
  END IF;
END;

-- ---------------------------------------------------------------------
-- Procedure: gibIntervall
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@GCP_PROJECT.@BQ_DATASET.gibIntervall`(
  IN Kennzahl STRING,
  OUT VarIntervall STRING,
  INOUT ErrNr INT64,
  INOUT ErrArg STRING
)
BEGIN
  IF ErrNr = 0 THEN
    IF Kennzahl IS NULL OR Kennzahl = '' OR VarIntervall IS NULL THEN
      SET ErrNr = 196;
      SET ErrArg = 'alis_parameter V8.3.1 gibIntervall';
    ELSE
      SET VarIntervall = CASE
        WHEN Kennzahl IN ('akti_sum', 'akti', 'apn', 'antwor', 'bds_vo', 'bst', 'bzug', 'babg', 'ivr_bzug', 'ivr_babg', 'd1n', 'eva_gp', 'eva_rd', 'eva_rv', 'eva_vt', 'frage', 'kamp', 'geo_bl', 'geo_gmd', 'geo_krs', 'geo_rgb', 'geo_stb', 'geo_str', 'glint', 'glv', 'gproz', 'gprs', 'ipdebs', 'ipdeb', 'kamp_seg', 'kart', 'kstl', 'lkl', 'lmo', 'lor', 'lsc', 'map_lk', 'morpu', 'morpu_tvd', 'morpu_lid', 'morpu_quell', 'morpu_bpr', 'morpu_param', 'morpu_attr', 'morpu_gru', 'morpu_mtc', 'morpu_preis', 'morpu_anzal', 'morpu_cwb_p', 'natint', 'nnk', 'nnkt', 'nnv_gprs', 'bpr_budget', 'nnv_budge', 'persu18', 'pktu', 'pln', 'pnkt_ab', 'pnkt_zg', 'pnktart', 'pnktzgd', 'produ', 'reselling', 'rub', 'prob_ford', 'prob_guts', 'preis_fac', 'fakt_fac', 'fact', 'nnv_mms', 'gvvk', 'gvvkgr', 'mmsz', 'mmsq', 'mmsr', 'nnv_fmn', 'sg_rv', 'spcap_whs', 'tln_ds', 'tln_sd', 'tnd', 'trfa', 'tvd', 'zonek', 'zonet', 'nnv_zelle', 'gdp_nutz', 'rv_imei') THEN 'm'
        WHEN Kennzahl IN ('abg', 'abz', 'akq', 'auf', 'budgets', 'bpi', 'bpi_zug_kond', 'bpi_abg_kond', 'basisd', 'bwa', 'artikel', 'cap', 'cgs', 'crs', 'crsk', 'crsb', 'crsr', 'cell_map', 'rpos_carm', 'crs_etg', 'crsx', 'crsxk', 'dgs', 'drs', 'etg', 'ft', 'gtyp', 'gut', 'gz', 'gvv_mms', 'gvv_gprs', 'volklasse', 'bpr_budge', 'budge_gza', 'gr_rpos', 'gr_nnvtvd', 'hist', 'ilv_ausn', 'itc_fa', 'idv_fa', 'tarif_var', 'map_apn', 'mccmnc', 'kes_autogv', 'ksd', 'ese', 'loe', 'ngakq_es', 'ngaktiv', 'ngvd_es', 'ngvorgang', 'ngzm_k4', 'ngfehl', 'ngrueck', 'ngfehlauf', 'mahn', 'mds', 'opal', 'paket', 'perf', 'pri', 'rak', 'rst', 'rpos_det', 'rep_x', 'scheck', 'sgs', 'sr_rv_dpps', 'srs', 'tkwp', 'tkwpp', 'tkwpt', 'tvk', 'twe', 'twvv_gv', 'twvv_e', 'twvv_e_oo', 'ust', 'usk', 'map_zelle', 'map_vprod', 'vbd', 've_all_b', 've_bp_z', 've_bp_a', 've_bp_b', 've_bprzv_z', 've_bprzv_a', 've_bprzv_b', 've_c2c_z', 've_c2c_a', 've_c2c_b', 've_ees_n1_whlg', 've_ees_n3_whlg', 've_ees_s0_init', 've_ees_s1_zpkt', 've_ees_s2_aufg', 've_ees_s3_kont', 've_ees_s4_kamp', 've_ees_s5_merge', 've_nv_z', 've_nv_a', 've_nv_b', 've_vvl_z', 've_vvl_a', 've_vvl_b', 've_vvlsp_z', 've_vvlsp_a', 've_vvlsp_b', 've_vvltm_z', 've_vvltm_a', 've_vvltm_b', 've_x2c_z', 've_x2c_a', 've_x2c_b', 'vorg', 'vorg2', 'vtg', 'plz_region', 'vvl', 'pos', 'wap', 'alter_sgmnt', 'nutz_haeuf', 'mms_zonet', 'vas_cont_ty', 'zeitz', 'zug', 'xtra_verfal') THEN 't'
        ELSE NULL
      END;

      IF VarIntervall IS NULL THEN
        SET ErrNr = 196;
        SET ErrArg = CONCAT('alis_parameter V8.3.1 gibIntervall - Kuerzel ', Kennzahl, ' unbekannt');
      END IF;
    END IF;
  END IF;
END;

-- ---------------------------------------------------------------------
-- Procedure: pruefeZeitraum
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@GCP_PROJECT.@BQ_DATASET.pruefeZeitraum`(
  IN Anfang STRING,
  IN Ende STRING,
  INOUT ErrNr INT64,
  INOUT ErrArg STRING
)
BEGIN
  DECLARE start_date DATE;
  DECLARE end_date DATE;

  IF ErrNr = 0 THEN
    IF Anfang IS NULL OR Anfang = '' OR Ende IS NULL OR Ende = '' THEN
      SET ErrNr = 196;
      SET ErrArg = 'alis_parameter V8.3.1 pruefeZeitraum';
    ELSE
      SET start_date = SAFE.PARSE_DATE('%Y%m%d', Anfang);
      SET end_date = SAFE.PARSE_DATE('%Y%m%d', Ende);

      IF start_date IS NULL THEN
        SET ErrNr = 195;
        SET ErrArg = 'Anfangsdatum entspricht nicht dem Format YYYYMMDD';
      ELSEIF end_date IS NULL THEN
        SET ErrNr = 195;
        SET ErrArg = 'Endedatum entspricht nicht dem Format YYYYMMDD';
      ELSEIF start_date > end_date THEN
        SET ErrNr = 195;
        SET ErrArg = 'Anfangsdatum ist nicht kleiner gleich Endedatum';
      END IF;
    END IF;
  END IF;
END;

-- ---------------------------------------------------------------------
-- Procedure: pruefeZahlPositiv
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@GCP_PROJECT.@BQ_DATASET.pruefeZahlPositiv`(
  IN p_Zahl INT64,
  IN p_ParameterName STRING,
  INOUT ErrNr INT64,
  INOUT ErrArg STRING
)
BEGIN
  IF p_Zahl IS NULL THEN
    SET ErrNr = 195;
    SET ErrArg = CONCAT('Parameter ', p_ParameterName, ' ist kein numerischer Wert');
  ELSEIF p_Zahl < 0 THEN
    SET ErrNr = 195;
    SET ErrArg = CONCAT('Parameter ', p_ParameterName, ' muss groesser gleich 0 sein');
  END IF;
END;

-- ---------------------------------------------------------------------
-- Procedure: pruefeZeitParameter
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@GCP_PROJECT.@BQ_DATASET.pruefeZeitParameter`(
  IN p_Anfangsdatum STRING,
  IN p_Endedatum STRING,
  IN p_ZeitOffset INT64,
  INOUT ErrNr INT64,
  INOUT ErrArg STRING
)
BEGIN
  IF ErrNr = 0 THEN
    IF p_ZeitOffset IS NOT NULL THEN
      IF (p_Anfangsdatum IS NULL OR p_Anfangsdatum = '') AND (p_Endedatum IS NULL OR p_Endedatum = '') THEN
        CALL `@GCP_PROJECT.@BQ_DATASET.pruefeZahlPositiv`(p_ZeitOffset, 'Zeitspanne', ErrNr, ErrArg);
      ELSE
        SET ErrNr = 195;
        SET ErrArg = 'Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden';
      END IF;
    ELSE
      IF (p_Anfangsdatum IS NOT NULL AND p_Anfangsdatum != '') AND (p_Endedatum IS NOT NULL AND p_Endedatum != '') THEN
        CALL `@GCP_PROJECT.@BQ_DATASET.pruefeZeitraum`(p_Anfangsdatum, p_Endedatum, ErrNr, ErrArg);
      ELSE
        SET ErrNr = 195;
        IF (p_Anfangsdatum IS NULL OR p_Anfangsdatum = '') AND (p_Endedatum IS NULL OR p_Endedatum = '') THEN
          SET ErrArg = 'Datumswerte oder Zeitspanne fehlen';
        ELSE
          SET ErrArg = 'Sowohl Anfang- als auch Endedatum muessen angegeben werden';
        END IF;
      END IF;
    END IF;
  END IF;
END;

-- ---------------------------------------------------------------------
-- Procedure: konvertiereZeitspanne
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@GCP_PROJECT.@BQ_DATASET.konvertiereZeitspanne`(
  INOUT p_VarAnfang STRING,
  INOUT p_VarEnde STRING,
  IN p_Spanne INT64,
  IN p_Kennzahl STRING,
  INOUT ErrNr INT64,
  INOUT ErrArg STRING
)
BEGIN
  DECLARE unit STRING;
  DECLARE base_date DATE;
  DECLARE start_date DATE;
  DECLARE end_date DATE;

  IF ErrNr = 0 THEN
    SET unit = IF(p_Kennzahl = 'bst', 'MONTH', 'DAY');
    SET base_date = CURRENT_DATE(); 

    IF unit = 'MONTH' THEN
      SET start_date = DATE_SUB(base_date, INTERVAL p_Spanne MONTH);
      SET end_date = base_date;
    ELSE
      SET start_date = DATE_SUB(base_date, INTERVAL p_Spanne DAY);
      SET end_date = base_date;
    END IF;

    SET p_VarAnfang = FORMAT_DATE('%Y%m%d', start_date);
    SET p_VarEnde = FORMAT_DATE('%Y%m%d', end_date);
  END IF;
END;

-- ---------------------------------------------------------------------
-- Procedure: DWPAR_SkriptPfad
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@GCP_PROJECT.@BQ_DATASET.DWPAR_SkriptPfad`(
  OUT v_VarName STRING,
  IN v_SkriptTyp STRING,
  IN v_Prozess STRING,
  IN v_System STRING,
  IN v_Daten STRING,
  IN DW_DIR_ROOT STRING,
  OUT return_code INT64
)
BEGIN
  DECLARE v_Endung STRING;
  DECLARE v_Dateiname STRING;
  DECLARE v_ProzessKurz STRING;

  SET v_Endung = IF(v_SkriptTyp = 'bin', 'ksh', v_SkriptTyp);

  SET v_ProzessKurz = CASE v_Prozess
    WHEN 'import' THEN 'ip'
    WHEN 'aufbereitung' THEN 'ab'
    WHEN 'exporter' THEN 'ex'
    WHEN 'verdichtung' THEN 'vd'
    WHEN 'allgemein' THEN 'al'
    WHEN 'pruef' THEN 'pf'
    WHEN 'vorverarbeitung' THEN 'vv'
    WHEN 'zulieferung' THEN 'zl'
    ELSE NULL
  END;

  IF v_ProzessKurz IS NULL THEN
    SET return_code = 2;
    SET v_VarName = NULL;
  ELSE
    SET v_Dateiname = CONCAT(DW_DIR_ROOT, '/', v_Prozess, '/', v_System, '/', v_SkriptTyp);
    
    IF v_SkriptTyp = 'bin' THEN
      SET v_Dateiname = CONCAT(v_Dateiname, '/k_', v_ProzessKurz, v_System, '_', v_Daten, '.', v_Endung);
    ELSE
      SET v_Dateiname = CONCAT(v_Dateiname, '/d_', v_ProzessKurz, v_System, '_', v_Daten, '.', v_Endung);
    END IF;

    SET v_VarName = v_Dateiname;
    SET return_code = 0; 
  END IF;
END;