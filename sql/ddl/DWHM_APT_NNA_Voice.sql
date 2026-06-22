-- DDL for target BigQuery table DWHM_APT_NNA_Voice
-- Legacy Source: EXIS_SD_APT_NNA_VOIC (various Oracle DWH tables via SQL)
-- Job: EXIS_SD_APT_NNA_VOIC

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.DWHM_APT_NNA_Voice` (
  MONATS_ID INT64 OPTIONS(description="Month ID (YYYYMM) from source NNA.MONATS_ID"),
  RAHMENVERTRAG STRING OPTIONS(description="Rahmenvertrag ID from source NNA.RAHMENVERTRAG"),
  MSISDN STRING OPTIONS(description="MSISDN from source VER.MSISDN"),
  KUNDENKONTO STRING OPTIONS(description="Customer Account Number from source VER.KUNDENKONTO"),
  T_MOBILE_KUNDENNUMMER STRING OPTIONS(description="T-Mobile Customer Number from source VER.T_MOBILE_KUNDENNUMMER"),
  TARIF_ID INT64 OPTIONS(description="Tariff ID from source TAR.TARIF_ID"),
  TARIF STRING OPTIONS(description="Tariff Description (MP_MARKTPRODUKT_BEZ,MP_EG_JN_BEZ,MP_GENERATION_BEZ)"),
  LEISTUNGSKLASSE_ID INT64 OPTIONS(description="Leistungsklasse ID from source TVD.LEISTUNGSKLASSE_ID"),
  LEISTUNGSKLASSE_TEXT STRING OPTIONS(description="Leistungsklasse Description from source TVD.LEISTUNGSKLASSE_TEXT"),
  VERBINDUNGEN INT64 OPTIONS(description="Number of Connections from source NNA.VERBINDUNGEN"),
  DAUER_MIN FLOAT64 OPTIONS(description="Duration in Minutes from source NNA.DAUER_SEK"),
  RBETRAG_VBUD_NETTO_EURO FLOAT64 OPTIONS(description="Netto Euro Amount from source NNA.RBETRAG_VBUD_NETTO_CENT"),
  MP_EG_JN_ID INT64 OPTIONS(description="MP_EG_JN_ID from source BL_D_TARIF"),
  MP_EG_JN_BEZ STRING OPTIONS(description="MP_EG_JN_BEZ from source BL_D_TARIF"),
  MP_GENERATION_ID INT64 OPTIONS(description="MP_GENERATION_ID from source BL_D_TARIF"),
  MP_GENERATION_BEZ STRING OPTIONS(description="MP_GENERATION_BEZ from source BL_D_TARIF")
)
OPTIONS(
  description = 'Data export of telephone system masterdata.'
);