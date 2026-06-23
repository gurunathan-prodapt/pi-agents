--
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_discount_rr.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
--
-- This script truncates and inserts data into the target table
-- `target_project.target_dataset.sof_ta_discount_rr`
-- from various `source_project.source_dataset.cds_` tables based on a provided date.
--
-- Parameters:
--   @v_datum_str: String in 'YYYYMMDD' format representing the processing date.

-- Truncate the target table before inserting new data.
TRUNCATE TABLE `target_project.target_dataset.sof_ta_discount_rr`;

INSERT INTO `target_project.target_dataset.sof_ta_discount_rr`(
        cntrct_id,
        discount_id,
        disc_vector_ty,
        cntrct_obj_version,
        cntrct_template_id,
        disc_invoice_item_id,
        rabatt,
        rabatthoehe,
        rabattierte_rech_pos
)
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
        `source_project.source_dataset.cds_ta_discount_bc_assoc` AS da
JOIN    `source_project.source_dataset.cds_ta_discount` AS d
  ON    da.discount_id = d.discount_id
JOIN    `source_project.source_dataset.cds_ta_care_description` AS cd
  ON    cd.cds_description_id = d.CDS_DESCRIPTION_ID
JOIN    `source_project.source_dataset.cds_ta_disc_vector` AS dv
  ON    d.discount_id = dv.discount_id
  AND   d.disc_vector_ty = dv.disc_vector_ty
  AND   d.obj_version = dv.discount_obj_version
JOIN    `source_project.source_dataset.cds_ta_disc_invoice_item` AS dii
  ON    d.DISC_INVOICE_ITEM_ID = dii.DISC_INVOICE_ITEM_ID
JOIN    `source_project.source_dataset.cds_ta_care_description` AS cdii
  ON    dii.CDS_DESCRIPTION_ID = cdii.CDS_DESCRIPTION_ID
WHERE
        cd.LANGUAGE = 1
AND
        cdii.LANGUAGE = 1
AND
        da.insert_at <= PARSE_DATE('%Y%m%d', @v_datum_str)
AND     (   da.modified_at IS NULL
         OR da.modified_at > PARSE_DATE('%Y%m%d', @v_datum_str) )
AND
        d.insert_at <= PARSE_DATE('%Y%m%d', @v_datum_str)
AND     (   d.modified_at IS NULL
         OR d.modified_at > PARSE_DATE('%Y%m%d', @v_datum_str) )
AND     d.valid_from <= PARSE_DATE('%Y%m%d', @v_datum_str)
AND     (   d.valid_to IS NULL
         OR d.valid_to > PARSE_DATE('%Y%m%d', @v_datum_str) )
AND
        dv.insert_at   <= PARSE_DATE('%Y%m%d', @v_datum_str)
AND     (   dv.modified_at IS NULL
         OR dv.modified_at > PARSE_DATE('%Y%m%d', @v_datum_str) )
AND     d.is_production = 1
AND
        dii.insert_at   <= PARSE_DATE('%Y%m%d', @v_datum_str)
AND     (   dii.modified_at IS NULL
         OR dii.modified_at > PARSE_DATE('%Y%m%d', @v_datum_str) );