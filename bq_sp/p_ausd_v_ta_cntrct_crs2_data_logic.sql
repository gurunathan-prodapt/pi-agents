-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs2.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
-- Description: BigQuery Stored Procedure for the core data processing logic, translated from d_ausd_v_ta_cntrct_crs2.sql.

CREATE OR REPLACE PROCEDURE project.dataset.p_ausd_v_ta_cntrct_crs2_data_logic(
    IN p_eintrags_nr STRING,
    IN p_job_kennung STRING,
    OUT v_record_count INT64
)
BEGIN
    DECLARE v_stichtag_yyyymmdd STRING;

    BEGIN
        -- Stichtag ermitteln
        -- Original: SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        SELECT
            IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
        INTO
            v_stichtag_yyyymmdd
        FROM
            project.dataset.dwtk_meldungen AS m
        WHERE
            m.job_kennung = 'BERT_DROP_TEMP_TABLE';

        -- tabelle von vorherigem lauf leeren
        -- Original: begin isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_crs2'); end;
        TRUNCATE TABLE project.dataset.sof_ta_cntrct_crs2;

        -- vertragstabelle mit rahmenvertrags-infos (ausgesiebt werden die rv-elternvertraege)
        -- Original SQL uses Oracle's outer join syntax (+)
        INSERT INTO project.dataset.sof_ta_cntrct_crs2(
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
            rv_num
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
            cr.contract_number AS RV_NUM
        FROM
            project.dataset.sof_ta_cntrct_crs AS c
        LEFT JOIN
            project.dataset.sof_ta_cntrct_crs AS cr
        ON
            c.cntrct_parent = cr.cntrct_id
            AND cr.cntrct_ty = 10 -- RV (Rahmenvertrag)
        WHERE
            c.cntrct_ty <> 10; -- Exclude RV parent contracts from the main selection

        -- Get record count
        SELECT COUNT(1) INTO v_record_count FROM project.dataset.sof_ta_cntrct_crs2;

    EXCEPTION WHEN ERROR THEN
        -- Log the error
        INSERT INTO project.dataset.error_log (job_id, entry_number, severity, message, procedure_name)
        VALUES (p_job_kennung, p_eintrags_nr, 'ERROR', CONCAT('Error in p_ausd_v_ta_cntrct_crs2_data_logic: ', @@error.message), 'p_ausd_v_ta_cntrct_crs2_data_logic');
        RAISE; -- Re-raise the error to the calling procedure
    END;
END;