-- Legacy status table: PoolBasisprodukt
-- Job: DW.BERT_AUSD_BP_TA_ICCID_VERTRAG

CREATE SCHEMA IF NOT EXISTS `gcp_project.dataset`;

CREATE TABLE IF NOT EXISTS `gcp_project.dataset.pool_basisprodukt`
(
    job_kennung STRING NOT NULL,
    status STRING,
    last_update TIMESTAMP
);