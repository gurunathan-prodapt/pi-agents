-- DDL for target table raw_sof.sof$ta_disc_zusgf
-- Replaces object creation in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh
-- and vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql
--
-- NOTE: Replace `your_gcp_project_id` with your actual Google Cloud Project ID.
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.raw_sof.sof$ta_disc_zusgf` (
    cntrct_id INT64,
    cntrct_obj_version INT64,
    disc_vector_ty STRING,
    rabatt_alle STRING
)
OPTIONS(
    description="Target table for consolidated discount data, migrated from Oracle sof$ta_disc_zusgf."
);