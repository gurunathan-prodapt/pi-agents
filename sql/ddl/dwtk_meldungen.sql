-- Legacy source table: isbert_schema.dwtk_meldungen
-- Job: DW.BERT_AUSD_BP_TA_ICCID_VERTRAG

CREATE SCHEMA IF NOT EXISTS `gcp_project.dataset`;

CREATE TABLE IF NOT EXISTS `gcp_project.dataset.dwtk_meldungen`
(
    job_kennung STRING NOT NULL,
    timecreated TIMESTAMP NOT NULL
);