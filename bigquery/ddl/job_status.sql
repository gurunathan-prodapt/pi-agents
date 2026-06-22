--
-- Status logging table for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
--
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_status`
(
    job_id STRING,
    status_time TIMESTAMP,
    status STRING
);