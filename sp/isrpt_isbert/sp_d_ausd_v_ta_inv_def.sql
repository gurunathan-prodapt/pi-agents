-- Migrated from legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_inv_def.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh

CREATE OR REPLACE PROCEDURE `my_gcp_project.isrpt_isbert.sp_d_ausd_v_ta_inv_def`(
    IN v_datum_str STRING,
    OUT records_processed INT64
)
OPTIONS(
    description="Migrated SQL transformation logic from d_ausd_v_ta_inv_def.sql to BigQuery. Processes invoice definitions."
)
BEGIN
    -- Legacy: isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_inv_def');
    -- Truncate the target table before inserting new data.
    TRUNCATE TABLE `my_gcp_project.isrpt_isbert.sof_ta_inv_def`;

    -- Insert into the target table, translating Oracle SQL to BigQuery SQL.
    INSERT INTO `my_gcp_project.isrpt_isbert.sof_ta_inv_def`(
        inv_definition_id,
        acc_ref_id,
        inv_pay_ty_cv,
        inv_media_cv,
        billcycle_id,
        sales_tax_freed,
        INV_CONT_CONFIG_ID,
        rechn_inh_konfig_text
    )
    SELECT
        id.inv_definition_id,
        id.acc_ref_id,
        id.inv_pay_ty_cv,
        id.inv_media_cv,
        id.billcycle_id,
        id.sales_tax_freed,
        id.INV_CONT_CONFIG_ID,
        d.CDS_DESCRIPTION AS rechn_inh_konfig_text
    FROM
        `my_gcp_project.isrpt_isbert.cds_ta_inv_definition` AS id
    LEFT JOIN
        `my_gcp_project.isrpt_isbert.cds_ta_inv_cont_config` AS icc
        ON id.INV_CONT_CONFIG_ID = icc.INV_CONT_CONFIG_ID
    LEFT JOIN
        `my_gcp_project.isrpt_isbert.cds_ta_care_description` AS d
        ON icc.CDS_DESCRIPTION_ID = d.CDS_DESCRIPTION_ID
    WHERE
        id.insert_at <= PARSE_DATE('%Y%m%d', v_datum_str)
        AND (id.modified_at IS NULL OR id.modified_at > PARSE_DATE('%Y%m%d', v_datum_str))
        AND id.valid_from <= PARSE_DATE('%Y%m%d', v_datum_str)
        AND (id.valid_to IS NULL OR id.valid_to > PARSE_DATE('%Y%m%d', v_datum_str))
        AND id.is_production = TRUE -- Assuming is_production is a boolean flag (0/1 in Oracle)
        -- Left join conditions for optional tables
        AND COALESCE(icc.insert_at, PARSE_DATE('%Y%m%d', '19000101')) <= PARSE_DATE('%Y%m%d', v_datum_str)
        AND COALESCE(icc.modified_at, PARSE_DATE('%Y%m%d', v_datum_str) + INTERVAL 1 DAY) > PARSE_DATE('%Y%m%d', v_datum_str)
        AND COALESCE(icc.valid_from, PARSE_DATE('%Y%m%d', '19000101')) <= PARSE_DATE('%Y%m%d', v_datum_str)
        AND COALESCE(icc.valid_to, PARSE_DATE('%Y%m%d', v_datum_str) + INTERVAL 1 DAY) > PARSE_DATE('%Y%m%d', v_datum_str)
        AND COALESCE(icc.is_production, FALSE) = TRUE; -- Assuming is_production is a boolean flag (0/1 in Oracle)

    SET records_processed = @@row_count;

    -- The original d_ausd_v_ta_inv_def.sql mentioned a MERGE statement into 'VIA'
    -- but the details of this MERGE were not present in the provided SQL.
    -- TODO: Add the MERGE statement into `my_gcp_project.isrpt_isbert.via` here once its logic is known.
    -- Example placeholder for MERGE (replace with actual logic):
    -- MERGE INTO `my_gcp_project.isrpt_isbert.via` AS T
    -- USING (
    --     SELECT
    --         inv_definition_id AS id,
    --         rechn_inh_konfig_text AS description
    --     FROM `my_gcp_project.isrpt_isbert.sof_ta_inv_def`
    -- ) AS S
    -- ON T.id = S.id
    -- WHEN MATCHED THEN
    --     UPDATE SET T.description = S.description, T.created_at = CURRENT_TIMESTAMP()
    -- WHEN NOT MATCHED THEN
    --     INSERT (id, description, created_at) VALUES (S.id, S.description, CURRENT_TIMESTAMP());

END;