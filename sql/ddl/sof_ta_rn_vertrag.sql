-- BigQuery DDL for sof_ta_rn_vertrag table
-- Based on the INSERT statement in d_ausd_bp_ta_rn_vertrag.sql
-- This table replaces Oracle table sof$ta_rn_vertrag
-- Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_rn_vertrag.sql
CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_rn_vertrag` (
    CNTRCT_ID STRING, -- Assuming STRING based on common ID types
    TN_MULTI_SINGLE STRING,
    TN_TEL_MSISDN STRING,
    TN_TEL_STATUS STRING,
    TN_TEL_VALID_TO DATE, -- Assuming date, adjust if TIMESTAMP needed
    TN_FAX_MSISDN STRING,
    TN_FAX_STATUS STRING,
    TN_FAX_VALID_TO DATE,
    TN_DAT_MSISDN STRING,
    TN_DAT_STATUS STRING,
    TN_DAT_VALID_TO DATE,
    TC_MULTI_SINGLE STRING,
    TC_TEL_MSISDN STRING,
    TC_TEL_STATUS STRING,
    TC_TEL_VALID_TO DATE,
    TC_FAX_MSISDN STRING,
    TC_FAX_STATUS STRING,
    TC_FAX_VALID_TO DATE,
    TC_DAT_MSISDN STRING,
    TC_DAT_STATUS STRING,
    TC_DAT_VALID_TO DATE,
    TB_MULTI_SINGLE STRING,
    TB_TEL_MSISDN STRING,
    TB_TEL_STATUS STRING,
    TB_TEL_VALID_TO DATE,
    TB_FAX_MSISDN STRING,
    TB_FAX_STATUS STRING,
    TB_FAX_VALID_TO DATE,
    TB_DAT_MSISDN STRING,
    TB_DAT_STATUS STRING,
    TB_DAT_VALID_TO DATE,
    MS_RN_1_MSISDN STRING,
    MS_RN_1_STATUS STRING,
    MS_RN_1_VALID_TO DATE,
    MS_RN_2_MSISDN STRING,
    MS_RN_2_STATUS STRING,
    MS_RN_2_VALID_TO DATE
);

-- DDL for project.dataset.dwtk_meldungen and project.dataset.sof_ta_rn_einzeln
-- Please ensure the schemas for these tables match their original Oracle counterparts.
-- Example placeholder (actual schema should be derived from source system):
/*
CREATE TABLE IF NOT EXISTS `project.dataset.dwtk_meldungen` (
    job_kennung STRING,
    timecreated TIMESTAMP,
    -- ... other columns
);

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_rn_einzeln` (
    cntrct_id STRING,
    TN_multi_single STRING,
    TN_TEL_msisdn STRING,
    TN_TEL_status STRING,
    TN_TEL_valid_to DATE,
    -- ... other columns matching the SELECT list
);
*/