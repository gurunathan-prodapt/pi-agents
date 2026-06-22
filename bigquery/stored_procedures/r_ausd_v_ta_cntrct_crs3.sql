-- Migrated from legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
-- Original SQL source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs3.sql

CREATE OR REPLACE PROCEDURE `my-project.my_dataset.r_ausd_v_ta_cntrct_crs3`(
  IN p_JobKennung STRING,
  IN p_EintragsNr INT64
)
BEGIN
  DECLARE v_datum STRING;
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_start_timestamp TIMESTAMP;

  -- Exception handling block
  BEGIN

    -- Determine the processing date from dwtk_meldungen
    SET v_datum = (
      SELECT
        COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
      FROM `my-project.my_dataset.dwtk_meldungen` AS m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    -- Log job start
    SET v_start_timestamp = CURRENT_TIMESTAMP();
    INSERT INTO `my-project.my_dataset.job_audit_log` (
        job_kennung,
        eintrags_nr,
        start_timestamp,
        status,
        message,
        records_processed,
        process_date
    ) VALUES (
        p_JobKennung,
        p_EintragsNr,
        v_start_timestamp,
        'STARTED',
        'Job started',
        0,
        PARSE_DATE('%Y%m%d', v_datum)
    );

    -- Truncate the target table
    TRUNCATE TABLE `my-project.my_dataset.sof_ta_cntrct_crs3`;

    -- Insert transformed data into the target table
    INSERT INTO `my-project.my_dataset.sof_ta_cntrct_crs3`(
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
        rv_num,
        twinbill,
        twin_vertrag_id
    )
    SELECT
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
        rv_num,
        twinbill,
        twin_vertrag_id
    FROM (
        -- First branch: primary contracts (not RV or Mobilfunkzusatzvertrag)
        -- Identify potential "Twinbill" children using LEFT JOIN
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
            c.rv_num,
            CASE WHEN ctb.cntrct_id IS NOT NULL THEN 'TB' END AS twinbill,
            ctb.cntrct_id AS twin_vertrag_id
        FROM
            `my-project.my_dataset.sof_ta_cntrct_crs2` AS c
        LEFT JOIN
            `my-project.my_dataset.sof_ta_cntrct_crs2` AS ctb
            ON c.cntrct_id = ctb.cntrct_parent AND ctb.cntrct_ty = 20
        WHERE
            c.cntrct_ty NOT IN (10, 20)

        UNION DISTINCT

        -- Second branch: Mobilfunkzusatzvertrag
        -- Always mark as 'TB' and link to its parent contract
        SELECT
            ctb.cntrct_id,
            ctb.obj_version,
            ctb.contract_number,
            ctb.cntrct_template_id,
            ctb.cntrct_validity_id,
            ctb.valid_from,
            ctb.com_per_ext_rea_cv,
            ctb.billcycle_id,
            ctb.vo_code,
            ctb.cntrct_start_date,
            ctb.cntrct_st,
            ctb.cntrct_parent,
            ctb.cntrct_ty,
            ctb.cost_centre,
            ctb.cost_centre_user,
            ctb.commitment_reference_date,
            ctb.order_number,
            c.rv_num, -- RV_NUM comes from the parent contract (c)
            'TB' AS twinbill,
            c.cntrct_id AS twin_vertrag_id
        FROM
            `my-project.my_dataset.sof_ta_cntrct_crs2` AS c
        INNER JOIN
            `my-project.my_dataset.sof_ta_cntrct_crs2` AS ctb
            ON c.cntrct_id = ctb.cntrct_parent
        WHERE
            ctb.cntrct_ty = 20 -- Mobilfunkzusatzvertrag
            AND c.cntrct_ty NOT IN (10, 20) -- Parent is not RV or Mobilfunkzusatzvertrag
    );
    SET v_records = @@row_count;

    -- Log job success
    INSERT INTO `my-project.my_dataset.job_audit_log` (
        job_kennung,
        eintrags_nr,
        start_timestamp,
        end_timestamp,
        status,
        message,
        records_processed,
        process_date
    ) VALUES (
        p_JobKennung,
        p_EintragsNr,
        v_start_timestamp,
        CURRENT_TIMESTAMP(),
        'SUCCESS',
        'Job completed successfully',
        v_records,
        PARSE_DATE('%Y%m%d', v_datum)
    );

  EXCEPTION WHEN ERROR THEN
    -- Log job failure
    INSERT INTO `my-project.my_dataset.job_audit_log` (
        job_kennung,
        eintrags_nr,
        start_timestamp,
        end_timestamp,
        status,
        message,
        records_processed,
        error_message,
        process_date
    ) VALUES (
        p_JobKennung,
        p_EintragsNr,
        v_start_timestamp,
        CURRENT_TIMESTAMP(),
        'FAILED',
        'Job failed',
        v_records,
        ERROR_MESSAGE(),
        PARSE_DATE('%Y%m%d', v_datum)
    );
    RAISE; -- Re-raise the error to propagate it
  END;
END;