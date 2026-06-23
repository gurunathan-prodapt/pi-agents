-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_discount_rr.sql
-- Description: BigQuery Stored Procedure for core data processing logic from d_ausd_v_ta_p_discount_rr.sql.

CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bigquery_dataset_id.d_ausd_v_ta_p_discount_rr`(
    IN p_job_kennung STRING,
    IN p_eintrags_nr INT64,
    OUT p_processed_records INT64
)
OPTIONS(
    description="Migrated core data processing logic for ta_p_discount_rr table. Replaces d_ausd_v_ta_p_discount_rr.sql."
)
BEGIN
    -- Declare variables
    DECLARE v_records_inserted INT64 DEFAULT 0;

    -- Legacy Oracle SQL included specific DEFINE, COLUMN, SPOOL, WHENEVER, SET commands.
    -- These are Oracle-specific and are not directly translated to BigQuery.
    --
    -- The Oracle script also determined a 'v_datum' from `isbert_schema.dwtk_meldungen`.
    -- Per comments in the legacy script, 'v_datum' was removed from table names.
    -- If 'v_datum' is still functionally relevant for dynamic filtering or logic,
    -- its equivalent logic needs to be implemented here. For this migration,
    -- it is assumed not to directly affect the main INSERT query.

    -- Original legacy TRUNCATE statement:
    -- begin isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_p_discount_rr'); end;
    TRUNCATE TABLE `your_gcp_project_id.your_bigquery_dataset_id.sof_ta_p_discount_rr`;

    -- Main data insertion logic from the original SQL script.
    -- Oracle-specific PARALLEL hints are removed.
    INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.sof_ta_p_discount_rr` (
        cntrct_id,
        discount_id,
        disc_vector_ty,
        cntrct_obj_version,
        cntrct_template_id,
        disc_invoice_item_id,
        rabatt,
        rabatthoehe,
        rabattierte_rech_pos,
        contract_number,
        std_vertrag
    )
    SELECT
        da.cntrct_id,
        da.discount_id,
        da.disc_vector_ty,
        da.cntrct_obj_version,
        da.cntrct_template_id,
        da.disc_invoice_item_id,
        da.rabatt,
        da.rabatthoehe,
        da.rabattierte_rech_pos,
        c.contract_number,
        ct.cds_description AS std_vertrag
    FROM
        `your_gcp_project_id.your_bigquery_dataset_id.sof_ta_discount_rr` AS da,
        `your_gcp_project_id.your_bigquery_dataset_id.sof_ta_cntrct_crs` AS c,
        `your_gcp_project_id.your_bigquery_dataset_id.sof_ta_cntrct_templ` AS ct
    WHERE
        da.cntrct_id            = c.cntrct_id
        AND da.cntrct_obj_version   = c.obj_version
        AND da.cntrct_template_id   = ct.cntrct_template_id;

    SET v_records_inserted = @@row_count;

    -- Set the OUT parameter
    SET p_processed_records = v_records_inserted;

    -- BigQuery implicitly commits transactions. Explicit COMMIT is not needed.

    -- The original script had SPOOL commands for tracing.
    -- For BigQuery, logging can be handled via Cloud Logging or by inserting
    -- entries into custom logging tables (e.g., job_audit).
END;