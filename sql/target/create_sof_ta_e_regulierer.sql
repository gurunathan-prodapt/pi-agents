-- DDL for BigQuery target table target.sof_ta_e_regulierer
-- Created by k_ausd_adressen.ksh (d_ausd_adressen.sql)
CREATE TABLE IF NOT EXISTS `PROJECT_ID.target.sof_ta_e_regulierer`
(
    inv_def_mopref_id           INT64,
    mop_bp_id                   INT64,
    means_of_payment_id         INT64
);