-- DDL for BigQuery representation of legacy Oracle table DWH$VI_L_MAP_FA_TARIF
-- Job: EXIS_SD_APT_NNA_VOIC

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.DWH_VI_L_MAP_FA_TARIF` (
  DWH_TARIF_ID INT64 OPTIONS(description="Data Warehouse Tariff ID"),
  TARIF_ID INT64 OPTIONS(description="Tariff ID"),
  GUELTIG_BIS DATE OPTIONS(description="Valid Until Date")
)
OPTIONS(
  description = 'BigQuery representation of legacy Oracle DWH$VI_L_MAP_FA_TARIF.'
);