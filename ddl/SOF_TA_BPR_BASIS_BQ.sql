-- Legacy source: d_ausd_bp_ta_iccid_einzeln.sql (invoked by k_ausd_bp_ta_iccid_einzeln.ksh)
-- BigQuery migration for job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
-- Placeholder DDL for SOF$TA_BPR_BASIS table.
-- Actual schema should be derived from the source Oracle database,
-- inferred columns based on usage in d_ausd_bp_ta_iccid_einzeln.sql.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bigquery_dataset.SOF_TA_BPR_BASIS_BQ` (
    `cntrct_id` STRING,         -- Inferred from SELECT clause
    `bpr_id` INT64,             -- Inferred from SELECT clause
    `iccid` STRING,             -- Inferred from SELECT clause
    `imsi_mcc` STRING,          -- Inferred from SELECT clause
    `imsi_mnc` STRING,          -- Inferred from SELECT clause
    `imsi_hlr` STRING,          -- Inferred from SELECT clause
    `imsi_si` STRING,           -- Inferred from SELECT clause
    `valid_to` DATE,            -- Inferred from SELECT clause
    `E_ID` STRING,              -- Inferred from SELECT clause
    `CARD_TYPE_NAME` STRING,    -- Inferred from SELECT clause
    `slave_number` INT64,       -- Inferred from SELECT clause
    -- Add other columns from SOF$TA_BPR_BASIS as needed
    `payload` JSON
);