-- DDL for BigQuery staging table staging.sof_ta_bp_ref_gp
-- Created by k_ausd_adressen.ksh (d_ausd_adressen.sql)
CREATE TABLE IF NOT EXISTS `PROJECT_ID.staging.sof_ta_bp_ref_gp`
(
    bp_id                       INT64,
    reachability_id             INT64,
    cntrct_cp2_id               INT64,
    inv_def_invrec_id           INT64,
    bpr_inst_evnrec_id          INT64,
    bpr_inst_srvusr_id          INT64
);