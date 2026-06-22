--
-- BigQuery Stored Procedure: project.dataset.ausd_bp_ta_cntrct_evn_core
-- Replaces core logic from k_ausd_bp_ta_cntrct_evn.ksh invoked by r_ausd_bp_ta_cntrct_evn.ksh
--
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_evn_core`(
  IN p_jobkennung STRING,
  IN p_stichtag DATE,
  IN p_job_nr INT64,
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- This procedure contains the actual SELECT, DELETE, INSERT statements
  -- to extract from DWH source tables and load into FOS target tables.
  -- The original logic from k_ausd_bp_ta_cntrct_evn.ksh needs to be
  -- translated and implemented here.
  -- As per the design document, this is currently a placeholder.

  -- Example: Delete existing data for restart (if applicable)
  -- This part needs to be carefully designed based on idempotency requirements
  -- and how the original script handled restarts for specific contract IDs.
  -- For now, commenting out as the exact logic for deletion is unknown.
  /*
  DELETE FROM `project.dataset.fos_target_table`
  WHERE DWH_VERTRAG_ID >= p_wiederanlaufWert
    AND STICH_TAG = p_stichtag; -- Assuming stichtag is part of the primary key for snapshots
  */

  -- Example: Insert new/updated data
  INSERT INTO `project.dataset.fos_target_table` (
    DWH_VERTRAG_ID,
    STICH_TAG,
    VERTRAGSNUMMER,
    PRODUKT_CODE,
    SCORE_RELEVANT_VALUE,
    LAST_UPDATE_TS
  )
  SELECT
    t.DWH_VERTRAG_ID,
    p_stichtag AS STICH_TAG,
    t.VERTRAGSNUMMER,
    t.PRODUKT_TYP AS PRODUKT_CODE, -- Example mapping
    t.BETRAG AS SCORE_RELEVANT_VALUE, -- Example mapping
    CURRENT_TIMESTAMP() AS LAST_UPDATE_TS
  FROM `project.dataset.dwh_ta_c_vertrag_source` AS t
  WHERE
    t.Gueltig_von <= p_stichtag
    AND p_stichtag < t.Gueltig_bis -- Assuming effective dating
    AND t.LADEDATUM <= p_stichtag -- Assuming data loaded before or on stichtag is relevant
    AND t.DWH_VERTRAG_ID > p_wiederanlaufWert; -- Apply restart filter

  INSERT INTO `project.dataset.job_log` (job_nr, job_kennung, log_ts, log_level, message)
  VALUES (p_job_nr, p_jobkennung, CURRENT_TIMESTAMP(), 'INFO', 'Core processing completed successfully.');

END;