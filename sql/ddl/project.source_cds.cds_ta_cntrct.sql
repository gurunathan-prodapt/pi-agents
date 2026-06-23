-- BigQuery DDL for project.source_cds.cds_ta_cntrct
-- Replaces Oracle table CDS$TA_CNTRCT
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh

CREATE TABLE IF NOT EXISTS `project.source_cds.cds_ta_cntrct`
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
    insert_at                       DATE,
    redundant_owner_id              INT64,
    modified_at                     DATE,
    valid_to                        DATE,
    is_production                   INT64
)
OPTIONS(
    description="Migrated CDS$TA_CNTRCT table from Oracle Carmen DB."
);