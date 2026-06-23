-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh

CREATE SCHEMA IF NOT EXISTS isbert_aufbereitung;

CREATE TABLE IF NOT EXISTS isbert_aufbereitung.configuration (
    config_key STRING NOT NULL PRIMARY KEY,
    config_value STRING NOT NULL,
    description STRING,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Example configuration entries (these would be inserted via DML)
-- INSERT INTO isbert_aufbereitung.configuration (config_key, config_value, description) VALUES
-- ('BERT_DIR_ROOT', 'vobs/dw_source/isrpt/isbert/SQL/aktuell', 'Root directory for BERT scripts (legacy path)'),
-- ('DEFAULT_LOG_PATH', 'gs://my-bigquery-logs-bucket/isbert/', 'Cloud Storage path for detailed logs if needed');