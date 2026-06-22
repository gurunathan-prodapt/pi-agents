--
-- DDL for BigQuery table: project.dataset.dwh_ta_c_vertrag_source
-- Placeholder for DWH source table referenced by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
-- This table needs to be populated with actual DWH data.
--
CREATE TABLE IF NOT EXISTS `project.dataset.dwh_ta_c_vertrag_source`
(
  DWH_VERTRAG_ID INT64 NOT NULL OPTIONS(description="Primary identifier for the contract in DWH"),
  Gueltig_von DATE NOT NULL OPTIONS(description="Validity start date of the record"),
  Gueltig_bis DATE NOT NULL OPTIONS(description="Validity end date of the record"),
  LADEDATUM DATE NOT NULL OPTIONS(description="Load date of the record into DWH"),
  -- Add other relevant columns from the original DWH$TA_C_VERTRAG table
  -- Example placeholders:
  VERTRAGSNUMMER STRING,
  KUNDEN_ID STRING,
  PRODUKT_TYP STRING,
  VERTRAGS_STATUS STRING,
  BETRAG NUMERIC,
  WAEHRUNG STRING
)
OPTIONS(
  description="Placeholder table for DWH source data related to contracts (TA_C_VERTRAG)."
);