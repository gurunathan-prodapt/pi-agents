-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
-- Purpose: Target analytics table for PoolBasisprodukt.

CREATE TABLE IF NOT EXISTS `project.dataset.PoolBasisprodukt` (
  stichtag DATE OPTIONS(description="Business date key representing the target partition date"),
  job_kennung STRING OPTIONS(description="Identifier of the executing job"),
  eintrags_nr STRING OPTIONS(description="Unique entry run number"),
  status STRING OPTIONS(description="Status of the loaded records"),
  created_at TIMESTAMP OPTIONS(description="Records ingestion timestamp")
);