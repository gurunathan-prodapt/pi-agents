--
-- Error logging table for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
--
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_error_log`
(
    job_id STRING,
    error_time TIMESTAMP,
    error_code INT64,
    error_message STRING,
    error_arg STRING
);