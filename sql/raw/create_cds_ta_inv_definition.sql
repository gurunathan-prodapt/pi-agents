-- DDL for BigQuery table raw.cds_ta_inv_definition
-- Replaces Oracle table cds$ta_inv_definition from k_ausd_adressen.ksh
CREATE TABLE IF NOT EXISTS `PROJECT_ID.raw.cds_ta_inv_definition`
(
    rdndnt_cp2_bp_id            INT64,
    rdndnt_cp2_reachability_id  INT64,
    inv_definition_id           INT64,
    insert_at                   TIMESTAMP,
    modified_at                 TIMESTAMP,
    valid_from                  TIMESTAMP,
    valid_to                    TIMESTAMP,
    is_production               INT64, -- Or BOOL
    rdndant_invrec              INT64
);