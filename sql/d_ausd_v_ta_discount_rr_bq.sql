-- BigQuery SQL transformation logic for d_ausd_v_ta_discount_rr.sql
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_discount_rr.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh
-- Note: This file contains only the core INSERT statement. It is designed to be called from a stored procedure.

-- This SQL is intended to be executed within a BigQuery Stored Procedure,
-- where `v_datum_str` is a declared variable.

INSERT INTO `your_project.curated_rpt.sof_ta_discount_rr`(
        cntrct_id,
        discount_id,
        disc_vector_ty,
        cntrct_obj_version,
        cntrct_template_id,
        disc_invoice_item_id,
        rabatt,
        rabatthoehe,
        rabattierte_rech_pos)
SELECT
        da.cntrct_id,
        da.discount_id,
        d.disc_vector_ty,
        da.cntrct_obj_version,
        d.cntrct_template_id,
        d.disc_invoice_item_id,
        cd.cds_description AS rabatt,
        dv.CALC_RULE_VALUE AS rabatthoehe,
        cdii.CDS_DESCRIPTION AS rabattierte_rech_pos
FROM
        `your_project.raw_isbert.cds_ta_discount_bc_assoc` AS da
INNER JOIN
        `your_project.raw_isbert.cds_ta_discount` AS d
ON
        da.discount_id          = d.discount_id
INNER JOIN
        `your_project.raw_isbert.cds_ta_care_description` AS cd
ON
        cd.cds_description_id   = d.cds_description_id
INNER JOIN
        `your_project.raw_isbert.cds_ta_disc_invoice_item` AS dii
ON
        d.disc_invoice_item_id  = dii.disc_invoice_item_id
INNER JOIN
        `your_project.raw_isbert.cds_ta_care_description` AS cdii
ON
        dii.cds_description_id  = cdii.cds_description_id
INNER JOIN
        `your_project.raw_isbert.cds_ta_disc_vector` AS dv
ON
        d.discount_id           = dv.discount_id
    AND d.disc_vector_ty        = dv.disc_vector_ty
    AND d.obj_version           = dv.discount_obj_version
WHERE
        cd.`language`             = 1
AND
        cdii.`language`           = 1
AND
        da.insert_at <= PARSE_TIMESTAMP('%Y%m%d', v_datum_str)
AND     (   da.modified_at IS NULL
         OR da.modified_at > PARSE_TIMESTAMP('%Y%m%d', v_datum_str) )
AND
        d.insert_at <= PARSE_TIMESTAMP('%Y%m%d', v_datum_str)
AND     (   d.modified_at IS NULL
         OR d.modified_at > PARSE_TIMESTAMP('%Y%m%d', v_datum_str) )
AND     d.valid_from <= PARSE_TIMESTAMP('%Y%m%d', v_datum_str)
AND     (   d.valid_to IS NULL
         OR d.valid_to > PARSE_TIMESTAMP('%Y%m%d', v_datum_str) )
AND
        dv.insert_at   <= PARSE_TIMESTAMP('%Y%m%d', v_datum_str)
AND     (   dv.modified_at IS NULL
         OR dv.modified_at > PARSE_TIMESTAMP('%Y%m%d', v_datum_str) )
AND     d.is_production = 1
AND
        dii.insert_at   <= PARSE_TIMESTAMP('%Y%m%d', v_datum_str)
AND     (   dii.modified_at IS NULL
         OR dii.modified_at > PARSE_TIMESTAMP('%Y%m%d', v_datum_str) );