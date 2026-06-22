-- Legacy Source: Various Oracle tables
-- Job: DW.BERT_ABLAUFSTEUERUNG
-- BigQuery DDL for source tables referenced in the migration.
-- Actual schemas for these tables need to be obtained from the source system.
-- These are placeholders for table creation with assumed basic types and columns identified in the SQL.

-- RPT$TA_S_D1_VERTRAG -> RPT_TA_S_D1_VERTRAG
CREATE TABLE IF NOT EXISTS `{{ var.value.SOURCE_BQ_PROJECT }}.{{ var.value.SOURCE_BQ_DATASET }}.RPT_TA_S_D1_VERTRAG` (
    RAHMENVERTRAG_ID STRING,
    SV_ID STRING,
    PARTNER_ID_CARMEN STRING,
    KUNDENKONTO STRING,
    MSISDN STRING,
    GEPLANT_KUEND DATE,
    BINDEFRIST DATE,
    VERTRAGSBEGINN DATE,
    VERTRAGSBINDUNG STRING,
    DWH_TARIFGR_TEXT STRING,
    VERTRAG_ID_CARMEN STRING,
    VERTRAGSSTATUS STRING -- Inferred from GROUP BY clause in source SQL
    -- Add other columns with appropriate types based on actual source schema
);

-- SOF$TA_BPR_OPTIONEN -> SOF_TA_BPR_OPTIONEN
CREATE TABLE IF NOT EXISTS `{{ var.value.SOURCE_BQ_PROJECT }}.{{ var.value.SOURCE_BQ_DATASET }}.SOF_TA_BPR_OPTIONEN` (
    CNTRCT_ID STRING,
    BPR_ID STRING
    -- Add other columns with appropriate types based on actual source schema
);

-- SOF$VI_L_OPTIONZUORDNUNG -> SOF_VI_L_OPTIONZUORDNUNG
CREATE TABLE IF NOT EXISTS `{{ var.value.SOURCE_BQ_PROJECT }}.{{ var.value.SOURCE_BQ_DATASET }}.SOF_VI_L_OPTIONZUORDNUNG` (
    OPTION_ID STRING
    -- Add other columns with appropriate types based on actual source schema
);

-- DWH$VI_L_MAP_FA_TARIF -> DWH_VI_L_MAP_FA_TARIF
CREATE TABLE IF NOT EXISTS `{{ var.value.SOURCE_BQ_PROJECT }}.{{ var.value.SOURCE_BQ_DATASET }}.DWH_VI_L_MAP_FA_TARIF` (
    DWH_TARIF_ID STRING,
    TARIF_ID STRING,
    GUELTIG_BIS DATE,
    MP_MARKTPRODUKT_BEZ STRING,
    MP_EG_JN_BEZ STRING,
    MP_GENERATION_BEZ STRING,
    MP_EG_JN_ID STRING,
    MP_GENERATION_ID STRING
    -- Add other columns with appropriate types based on actual source schema
);

-- BL_D_TARIF
CREATE TABLE IF NOT EXISTS `{{ var.value.SOURCE_BQ_PROJECT }}.{{ var.value.SOURCE_BQ_DATASET }}.BL_D_TARIF` (
    TARIF_ID STRING,
    MP_MARKTPRODUKT_BEZ STRING,
    MP_EG_JN_BEZ STRING,
    MP_GENERATION_BEZ STRING,
    MP_EG_JN_ID STRING,
    MP_GENERATION_ID STRING
    -- Add other columns with appropriate types based on actual source schema
);

-- DWH$VI_C_VERTRAG -> DWH_VI_C_VERTRAG
CREATE TABLE IF NOT EXISTS `{{ var.value.SOURCE_BQ_PROJECT }}.{{ var.value.SOURCE_BQ_DATASET }}.DWH_VI_C_VERTRAG` (
    DWH_VERTRAG_ID STRING,
    DWH_TARIF_ID STRING,
    MSISDN STRING,
    KUNDENKONTO STRING,
    T_MOBILE_KUNDENNUMMER STRING
    -- Add other columns with appropriate types based on actual source schema
);

-- DWH$TA_F_NNV_GPRS -> DWH_TA_F_NNV_GPRS
CREATE TABLE IF NOT EXISTS `{{ var.value.SOURCE_BQ_PROJECT }}.{{ var.value.SOURCE_BQ_DATASET }}.DWH_TA_F_NNV_GPRS` (
    MONATS_ID INT64,
    RAHMENVERTRAG STRING,
    MSISDN STRING,
    AUSLAND_FLAG STRING,
    GESAMTVOLUMEN_BYTE BIGNUMERIC,
    RBETRAG_VBUD_NETTO_CENT_VOL BIGNUMERIC,
    DWH_VERTRAG_ID STRING
    -- Add other columns with appropriate types based on actual source schema
);

-- DWH$VI_F_NNV_TVD_12_MONATE -> DWH_VI_F_NNV_TVD_12_MONATE
CREATE TABLE IF NOT EXISTS `{{ var.value.SOURCE_BQ_PROJECT }}.{{ var.value.SOURCE_BQ_DATASET }}.DWH_VI_F_NNV_TVD_12_MONATE` (
    MONATS_ID INT64,
    RAHMENVERTRAG STRING,
    VERBINDUNGEN INT64,
    DAUER_SEK INT64,
    RBETRAG_VBUD_NETTO_CENT BIGNUMERIC,
    LEISTUNGSKLASSE_ID INT64,
    DWH_VERTRAG_ID STRING
    -- Add other columns with appropriate types based on actual source schema
);

-- DWH$VI_L_TVD_LEISTUNGSKLASSE -> DWH_VI_L_TVD_LEISTUNGSKLASSE
CREATE TABLE IF NOT EXISTS `{{ var.value.SOURCE_BQ_PROJECT }}.{{ var.value.SOURCE_BQ_DATASET }}.DWH_VI_L_TVD_LEISTUNGSKLASSE` (
    LEISTUNGSKLASSE_ID INT64,
    LEISTUNGSKLASSE_TEXT STRING,
    LEISTUNGSKLASSEGR_ID INT64
    -- Add other columns with appropriate types based on actual source schema
);

-- RPT$TA_S_D1_DISCOUNT_RR -> RPT_TA_S_D1_DISCOUNT_RR
CREATE TABLE IF NOT EXISTS `{{ var.value.SOURCE_BQ_PROJECT }}.{{ var.value.SOURCE_BQ_DATASET }}.RPT_TA_S_D1_DISCOUNT_RR` (
    CONTRACT_NUMBER STRING,
    CNTRCT_TEMPLATE_ID STRING,
    RABATTIERTE_RECH_POS STRING,
    DISC_INVOICE_ITEM_ID STRING,
    RABATTHOEHE BIGNUMERIC
    -- Add other columns with appropriate types based on actual source schema
);