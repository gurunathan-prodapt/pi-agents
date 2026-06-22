-- DDL for BigQuery table raw.bpd_ta_reachability
-- Replaces Oracle table bpd$ta_reachability from k_ausd_adressen.ksh
CREATE TABLE IF NOT EXISTS `PROJECT_ID.raw.bpd_ta_reachability`
(
    bp_id                       INT64,
    reachability_id             INT64,
    obj_version                 INT64,
    country_code                STRING,
    for_the_attention_of        STRING,
    address_attachment          STRING,
    address_attachment_org      STRING,
    corp_unit                   STRING,
    surname_s                   STRING,
    first_name_g                STRING,
    zip_code                    STRING,
    city                        STRING,
    pobox                       STRING,
    street                      STRING,
    house_nr                    STRING,
    public_area_a               STRING,
    private_area_p              STRING,
    corp_unit_ou1               STRING,
    address_line_1              STRING,
    address_line_2              STRING,
    reachable_from              TIMESTAMP,
    reachable_thru              TIMESTAMP,
    insert_at                   TIMESTAMP,
    modified_at                 TIMESTAMP,
    valid_from                  TIMESTAMP,
    valid_to                    TIMESTAMP,
    is_production               INT64  -- Or BOOL
);