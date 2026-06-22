-- DDL for BigQuery representation of legacy Oracle table DWH$VI_L_TVD_LEISTUNGSKLASSE
-- Job: EXIS_SD_APT_NNA_VOIC

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.DWH_VI_L_TVD_LEISTUNGSKLASSE` (
  LEISTUNGSKLASSE_ID INT64 OPTIONS(description="Leistungsklasse ID"),
  LEISTUNGSKLASSEGR_ID INT64 OPTIONS(description="Leistungsklasse Group ID"),
  LEISTUNGSKLASSE_TEXT STRING OPTIONS(description="Leistungsklasse Description")
)
OPTIONS(
  description = 'BigQuery representation of legacy Oracle DWH$VI_L_TVD_LEISTUNGSKLASSE.'
);