--
-- DDL for BigQuery table: project.dataset.fos_target_table
-- Placeholder for FOS target table populated by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.fos_target_table`
(
  DWH_VERTRAG_ID INT64 NOT NULL OPTIONS(description="Identifier for the contract, derived from DWH"),
  STICH_TAG DATE NOT NULL OPTIONS(description="Stichtag (cutoff date) for the data snapshot"),
  -- Add other relevant columns that will be extracted and transformed
  -- Example placeholders:
  VERTRAGSNUMMER STRING,
  PRODUKT_CODE STRING,
  SCORE_RELEVANT_VALUE NUMERIC,
  LAST_UPDATE_TS TIMESTAMP
)
OPTIONS(
  description="Placeholder table for the target FOS staging data, prepared for demand scoring."
);