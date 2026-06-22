--
-- BigQuery DDL for sof_dataset.ta_cntrct_crs3
-- Legacy Source: sof$ta_cntrct_crs3 (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `sof_dataset`;

CREATE TABLE IF NOT EXISTS `sof_dataset.ta_cntrct_crs3`
(
    `cntrct_id` INT64 NOT NULL,
    `cntrct_ty` INT64,
    `rv_num` STRING,
    `vo_code` STRING,
    `order_number` STRING,
    `cntrct_start_date` DATE,
    `cntrct_st` INT64,
    `twinbill` STRING,
    `cntrct_template_id` INT64,
    `contract_number` STRING,
    `cost_centre` STRING,
    `cost_centre_user` STRING,
    `commitment_reference_date` DATE,
    `cntrct_validity_id` INT64,
    `cntrct_parent` INT64, -- Added based on second UNION ALL clause
    `cntrct_cp2_id` INT64 -- Added based on first UNION ALL clause
)
OPTIONS(
    description="BigQuery equivalent of Oracle table sof$ta_cntrct_crs3"
);