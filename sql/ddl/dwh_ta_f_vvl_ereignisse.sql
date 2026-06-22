-- Migrated DDL for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
-- Legacy Source: DWH$TA_F_VVL_EREIGNISSE
--
-- This is a placeholder schema, inferred from the INSERT statement in d_ausd_v_ta_vvl_dwh.sql.
-- Please review and update with actual column definitions and data types.
--
CREATE TABLE IF NOT EXISTS `project_id.isbert_source_dataset.dwh_ta_f_vvl_ereignisse`
(
    stichtag                 DATE,
    vertrags_id              STRING, -- Assuming string, could be INT64
    dwh_vertrag_id           STRING,
    vo_kenn                  STRING,
    rahmenvertrag            STRING,
    dwh_tarifgr_id           STRING,
    aenderung_am             TIMESTAMP,
    vvl_aendgrund_id         INT64,
    vvl_crd_alt              STRING,
    vvl_ersteperiode_alt     STRING,
    vvl_folgeperiode_alt     STRING,
    vertragsbindedatum_alt   DATE,
    vvl_crd_neu              STRING,
    vvl_ersteperiode_neu     STRING,
    vvl_folgeperiode_neu     STRING,
    vertragsbindedatum_neu   DATE,
    vertragsbeginn           DATE,
    ladedatum                TIMESTAMP,
    vo_kenn_bearb            STRING,
    vb_kenn_bearb            STRING,
    vb_kenn                  STRING,
    kd_segment_id            STRING,
    vt_segment_id            STRING,
    rd_segment_id            STRING,
    ads_user_id              STRING,
    cks_objekt_id            STRING,
    kkm_kampagne_id          STRING,
    cks_artikel_ausgegeben   STRING,
    cks_bearb_kenn           STRING,
    ve_kamp_anrtyp_id        STRING,
    kkm_kontakt_id           STRING,
    vorgang_id               STRING,
    import_status_flag       STRING,
    dwh_tarif_id             STRING
    -- Add other columns if any, with appropriate BigQuery data types
);