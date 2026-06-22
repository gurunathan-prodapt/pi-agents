-- Migrated SQL from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_vvl_dwh.sql
-- Original Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
--
-- This script has been translated to BigQuery Standard SQL.
-- Placeholder dataset names 'project_id.isbert_source_dataset' and 'project_id.target_dataset' are used.
-- Please review and replace with actual project and dataset IDs.
--
-- Original Oracle SQL comments and SQL*Plus specific commands have been removed or commented out.
--
-- TO DO:
-- 1. Review and refine column data types for all tables based on source system's exact schema.
-- 2. The PL/SQL package call 'isbert_schema.DWPA_UTIL_SKRIPT.runstatement' needs to be re-implemented.
--    For TRUNCATE, use DELETE FROM `project_id.target_dataset.sof_ta_vvl_dwh` WHERE TRUE;
--    The 'runstatement' method's full functionality needs to be analyzed and recreated in BigQuery UDFs/Stored Procedures or Airflow Python tasks.
-- 3. Parameter handling (e.g., v_datum) from the original script should be managed by Airflow.
-- 4. Error handling and logging (`DWMSG_MeldeFehler`) should be handled by Airflow's native mechanisms.

-- Set a default value for v_datum if not passed as a parameter from Airflow
-- This logic was originally used to determine a date from DWTK_MELDUNGEN.
-- In BigQuery, this should be handled by Airflow passing the date or
-- recreating the logic directly if necessary.
-- DECLARE v_datum STRING DEFAULT FORMAT_DATE('%Y%m%d', CURRENT_DATE());
-- SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
--   FROM `project_id.isbert_source_dataset.dwtk_meldungen` m
--  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

-- Truncate equivalent in BigQuery (DELETE all rows)
DELETE FROM `project_id.target_dataset.sof_ta_vvl_dwh`
WHERE TRUE;

-- Insert data into target table
INSERT INTO `project_id.target_dataset.sof_ta_vvl_dwh`(
   stichtag,
   vertrags_id,
   dwh_vertrag_id,
   vo_kenn,
   rahmenvertrag,
   dwh_tarifgr_id,
   aenderung_am,
   vvl_aendgrund_id,
   vvl_crd_alt,
   vvl_ersteperiode_alt,
   vvl_folgeperiode_alt,
   vertragsbindedatum_alt,
   vvl_crd_neu,
   vvl_ersteperiode_neu,
   vvl_folgeperiode_neu,
   vertragsbindedatum_neu,
   vertragsbeginn,
   ladedatum,
   vo_kenn_bearb,
   vb_kenn_bearb,
   vb_kenn,
   kd_segment_id,
   vt_segment_id,
   rd_segment_id,
   ads_user_id,
   cks_objekt_id,
   kkm_kampagne_id,
   cks_artikel_ausgegeben,
   cks_bearb_kenn,
   ve_kamp_anrtyp_id,
   kkm_kontakt_id,
   vorgang_id,
   import_status_flag,
   dwh_tarif_id
)
SELECT
   stichtag,
   vertrags_id,
   dwh_vertrag_id,
   vo_kenn,
   rahmenvertrag,
   dwh_tarifgr_id,
   aenderung_am,
   vvl_aendgrund_id,
   vvl_crd_alt,
   vvl_ersteperiode_alt,
   vvl_folgeperiode_alt,
   vertragsbindedatum_alt,
   vvl_crd_neu,
   vvl_ersteperiode_neu,
   vvl_folgeperiode_neu,
   vertragsbindedatum_neu,
   vertragsbeginn,
   ladedatum,
   vo_kenn_bearb,
   vb_kenn_bearb,
   vb_kenn,
   kd_segment_id,
   vt_segment_id,
   rd_segment_id,
   ads_user_id,
   cks_objekt_id,
   kkm_kampagne_id,
   cks_artikel_ausgegeben,
   cks_bearb_kenn,
   ve_kamp_anrtyp_id,
   kkm_kontakt_id,
   vorgang_id,
   import_status_flag,
   dwh_tarif_id
FROM `project_id.isbert_source_dataset.dwh_ta_f_vvl_ereignisse` AS vvl
WHERE
    (
        vvl.vvl_aendgrund_id IN ( -3, 6, 7, 12, 13, 14, 15, 16, 17, 22, 80)
        OR vvl.vvl_aendgrund_id BETWEEN 24 AND 60
    );