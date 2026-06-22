--
-- Logging table for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
--
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_log`
(
    job_id STRING,
    start_time TIMESTAMP,
    status STRING,
    message STRING
);