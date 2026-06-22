-- DDL for BigQuery table raw.cds_ta_bp_ref
-- Replaces Oracle table cds$ta_bp_ref from k_ausd_adressen.ksh
CREATE TABLE IF NOT EXISTS `PROJECT_ID.raw.cds_ta_bp_ref`
(
    bp_id                       INT64,
    reachability_id             INT64,
    cntrct_cp2_id               INT64,
    inv_def_invrec_id           INT64,
    bpr_inst_evnrec_id          INT64,
    bpr_inst_srvusr_id          INT64,
    insert_at                   TIMESTAMP,
    modified_at                 TIMESTAMP,
    valid_from                  TIMESTAMP,
    valid_to                    TIMESTAMP,
    is_production               INT64, -- Or BOOL, based on exact data
    bp_ref_ty                   INT64,
    address_ref_ty              INT64,
    inv_def_mopref_id           INT64,
    mop_bp_id                   INT64,
    means_of_payment_id         INT64,
    mop_ref_ty                  INT64
);