--
-- BigQuery DDL for sof_dataset.ta_inv_acc
-- Legacy Source: sof$ta_inv_acc (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `sof_dataset`;

CREATE TABLE IF NOT EXISTS `sof_dataset.ta_inv_acc`
(
    `cntrct_id` INT64 NOT NULL,
    `inv_definition_id` INT64,
    `account_reference` STRING,
    `sales_tax_freed` STRING,
    `billcycle_id` STRING,
    `inv_pay_ty_cv` INT64,
    `inv_media_cv` INT64,
    `rechn_inh_konfig_text` STRING
)
OPTIONS(
    description="BigQuery equivalent of Oracle table sof$ta_inv_acc"
);