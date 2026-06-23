-- BigQuery DDL for project.staging.sof_ta_cntrct_crs
-- Replaces Oracle table SOF$TA_CNTRCT_CRS
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh

CREATE TABLE IF NOT EXISTS `project.staging.sof_ta_cntrct_crs`
(
    cntrct_id                       INT64,
    obj_version                     INT64,
    contract_number                 STRING,
    cntrct_template_id              INT64,
    cntrct_validity_id              INT64,
    valid_from                      DATE,
    com_per_ext_rea_cv              STRING,
    billcycle_id                    INT64,
    vo_code                         STRING,
    cntrct_start_date               DATE,
    cntrct_st                       INT64,
    cntrct_parent                   INT64,
    cntrct_ty                       INT64,
    cost_centre                     STRING,
    cost_centre_user                STRING,
    commitment_reference_date       DATE,
    order_number                    STRING,
    bfc_age                         DATE -- Derived from c.insert_at
)
OPTIONS(
    description="Target staging table for processed contract data."
);