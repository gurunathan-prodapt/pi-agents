-- BigQuery DDL for raw_dwh.VI_L_MAP_FA_TARIF
-- Replaces source Oracle table DWH$VI_L_MAP_FA_TARIF for job EXIS_SD_APT_NNA_VOIC

CREATE TABLE IF NOT EXISTS `your-gcp-project-id.raw_dwh.VI_L_MAP_FA_TARIF` (
    DWH_TARIF_ID INT64 NOT NULL,
    TARIF_ID INT64 NOT NULL,
    GUELTIG_BIS DATE,
    _LAST_MODIFIED_TS TIMESTAMP OPTIONS(description="Last modified timestamp for row tracking")
);