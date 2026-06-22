-- DDL for BigQuery representation of legacy Oracle table DWH$VI_F_NNV_TVD_12_MONATE
-- Job: EXIS_SD_APT_NNA_VOIC

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.DWH_VI_F_NNV_TVD_12_MONATE` (
  DWH_VERTRAG_ID INT64 OPTIONS(description="Data Warehouse Contract ID"),
  MONATS_ID INT64 OPTIONS(description="Month ID (YYYYMM)"),
  RAHMENVERTRAG STRING OPTIONS(description="Rahmenvertrag ID"),
  LEISTUNGSKLASSE_ID INT64 OPTIONS(description="Leistungsklasse ID"),
  VERBINDUNGEN INT64 OPTIONS(description="Number of Connections"),
  DAUER_SEK INT64 OPTIONS(description="Duration in Seconds"),
  RBETRAG_VBUD_NETTO_CENT INT64 OPTIONS(description="Netto Euro Amount in Cents")
)
OPTIONS(
  description = 'BigQuery representation of legacy Oracle DWH$VI_F_NNV_TVD_12_MONATE.'
);