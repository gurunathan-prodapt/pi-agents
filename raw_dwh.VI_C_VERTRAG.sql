-- BigQuery DDL for raw_dwh.VI_C_VERTRAG
-- Replaces source Oracle table DWH$VI_C_VERTRAG for job EXIS_SD_APT_NNA_VOIC

CREATE TABLE IF NOT EXISTS `your-gcp-project-id.raw_dwh.VI_C_VERTRAG` (
    DWH_TARIF_ID INT64 NOT NULL,
    DWH_VERTRAG_ID INT64 NOT NULL,
    MSISDN STRING,
    KUNDENKONTO STRING,
    T_MOBILE_KUNDENNUMMER STRING,
    _LAST_MODIFIED_TS TIMESTAMP OPTIONS(description="Last modified timestamp for row tracking")
);