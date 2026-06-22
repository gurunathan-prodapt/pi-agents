--
-- Result logging table for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
--
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_result`
(
    job_id STRING,
    end_time TIMESTAMP,
    records_processed INT64,
    status STRING
);