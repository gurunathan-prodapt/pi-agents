-- BigQuery Stored Procedure for Data Transformation
-- Replaces Oracle SQL script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs.sql
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh

CREATE OR REPLACE PROCEDURE `project.staging.d_ausd_v_ta_cntrct_crs`(
    v_datum DATE
)
BEGIN
    -- Truncate the target table
    TRUNCATE TABLE `project.staging.sof_ta_cntrct_crs`;

    -- Insert data into the target table
    INSERT INTO `project.staging.sof_ta_cntrct_crs`
    (
      cntrct_id,
      obj_version,
      contract_number,
      cntrct_template_id,
      cntrct_validity_id,
      valid_from,
      com_per_ext_rea_cv,
      billcycle_id,
      vo_code,
      cntrct_start_date,
      cntrct_st,
      cntrct_parent,
      cntrct_ty,
      cost_centre,
      cost_centre_user,
      commitment_reference_date,
      order_number,
      bfc_age
    )
    SELECT
      c.cntrct_id,
      c.obj_version,
      c.contract_number,
      c.cntrct_template_id,
      c.cntrct_validity_id,
      c.valid_from,
      c.com_per_ext_rea_cv,
      c.billcycle_id,
      c.vo_code,
      c.cntrct_start_date,
      c.cntrct_st,
      c.cntrct_parent,
      c.cntrct_ty,
      c.cost_centre,
      c.cost_centre_user,
      c.commitment_reference_date,
      c.order_number,
      c.insert_at AS bfc_age -- bfc_age is derived from c.insert_at
    FROM
      `project.source_cds.cds_ta_cntrct` c
    WHERE
      c.cntrct_st IN (5, 6) -- nur Vertragsstatus aktiv und beendet (d.h. reaktivierbar)
      AND c.redundant_owner_id = 1 -- keine Service Provider Vertraege
      AND c.insert_at <= v_datum
      AND (c.modified_at IS NULL OR c.modified_at > v_datum)
      AND c.valid_from <= v_datum
      AND (c.valid_to IS NULL OR c.valid_to > v_datum)
      AND c.is_production = 1
      AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL);
END;