-- DDL for BigQuery representation of legacy Oracle table DWH$VI_C_VERTRAG
-- Job: EXIS_SD_APT_NNA_VOIC

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.DWH_VI_C_VERTRAG` (
  DWH_TARIF_ID INT64 OPTIONS(description="Data Warehouse Tariff ID"),
  DWH_VERTRAG_ID INT64 OPTIONS(description="Data Warehouse Contract ID"),
  MSISDN STRING OPTIONS(description="MSISDN"),
  KUNDENKONTO STRING OPTIONS(description="Customer Account Number"),
  T_MOBILE_KUNDENNUMMER STRING OPTIONS(description="T-Mobile Customer Number")
)
OPTIONS(
  description = 'BigQuery representation of legacy Oracle DWH$VI_C_VERTRAG.'
);