-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.ta_cntrct_valid` (
    cntrct_validity_id STRING,
    first_period_id STRING,
    following_period_id STRING,
    first_notice_period_id STRING,
    follow_notice_period_id STRING,
    bfc_age TIMESTAMP,
    -- Add any other relevant columns if known from source system or design
    -- For example, audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);