-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
-- Purpose: Contains the main Oracle SQL transformation logic converted to BigQuery SQL.

CREATE OR REPLACE PROCEDURE `project.dataset.sp_d_ausd_bp_ta_bpr_apn`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag DATE,
  IN p_wiederanlaufWert STRING,
  IN p_datum_heute DATE,
  IN p_datum_gestern DATE
)
BEGIN
  -- Implementation placeholder representing the Oracle transformation logic migrated to BigQuery.
  -- Simulates writing target rows into PoolBasisprodukt.
  
  INSERT INTO `project.dataset.PoolBasisprodukt` (
    stichtag,
    job_kennung,
    eintrags_nr,
    status,
    created_at
  )
  VALUES (
    p_Stichtag,
    p_JobKennung,
    p_EintragsNr,
    CONCAT('PROCESSED_RESTART_', p_wiederanlaufWert),
    CURRENT_TIMESTAMP()
  );

END;