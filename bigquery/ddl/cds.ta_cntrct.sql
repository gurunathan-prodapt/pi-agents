-- DDL for table `cds.ta_cntrct`
-- Inferred from usage in d_ausd_bp_ta_bpr_basis_his.sql
-- Original job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

CREATE TABLE IF NOT EXISTS `your-gcp-project.cds.ta_cntrct` (
    cntrct_id STRING NOT NULL,
    cntrct_st INT64,
    redundant_owner_id INT64,
    insert_at DATE,
    modified_at DATE,
    valid_from DATE,
    valid_to DATE,
    is_production INT64,
    cntrct_ty INT64,
    cntrct_parent STRING
    -- Add other columns as per actual source schema
);