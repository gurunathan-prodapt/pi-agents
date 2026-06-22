-- DDL for BigQuery representation of legacy Oracle table BL_D_TARIF
-- Job: EXIS_SD_APT_NNA_VOIC

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.BL_D_TARIF` (
  TARIF_ID INT64 OPTIONS(description="Tariff ID"),
  MP_MARKTPRODUKT_BEZ STRING OPTIONS(description="Market Product Description"),
  MP_EG_JN_BEZ STRING OPTIONS(description="MP EG JN Description"),
  MP_GENERATION_BEZ STRING OPTIONS(description="MP Generation Description"),
  MP_EG_JN_ID INT64 OPTIONS(description="MP EG JN ID"),
  MP_GENERATION_ID INT64 OPTIONS(description="MP Generation ID")
)
OPTIONS(
  description = 'BigQuery representation of legacy Oracle BL_D_TARIF.'
);