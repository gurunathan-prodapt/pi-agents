-- BigQuery DDL for raw_dwh.BL_D_TARIF
-- Replaces source Oracle table BL_D_TARIF for job EXIS_SD_APT_NNA_VOIC

CREATE TABLE IF NOT EXISTS `your-gcp-project-id.raw_dwh.BL_D_TARIF` (
    TARIF_ID INT64 NOT NULL,
    MP_MARKTPRODUKT_BEZ STRING,
    MP_EG_JN_BEZ STRING,
    MP_GENERATION_BEZ STRING,
    MP_EG_JN_ID INT64,
    MP_GENERATION_ID INT64,
    _LAST_MODIFIED_TS TIMESTAMP OPTIONS(description="Last modified timestamp for row tracking")
);