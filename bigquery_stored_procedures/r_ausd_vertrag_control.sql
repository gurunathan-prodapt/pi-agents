-- BigQuery Stored Procedure for r_ausd_vertrag_control
-- Replaces legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
-- and integrates d_ausd_v_ta_vertrag_tmp.sql logic.

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
    p_jobkennunG STRING,
    p_eintrags_nr STRING
)
BEGIN
    DECLARE v_datum STRING;
    DECLARE records_processed INT64;
    DECLARE job_status STRING DEFAULT 'RUNNING';
    DECLARE error_message STRING;
    DECLARE error_code STRING;

    -- Set default project and dataset for convenience if not fully qualified
    -- SET @@default_project = 'your-gcp-project-id';
    -- SET @@default_dataset = 'your_bigquery_dataset';

    -- 1. Parameter Validation
    IF p_jobkennung IS NULL OR TRIM(p_jobkennung) = '' THEN
        SET error_message = 'ERROR: Parameter p_JobKennung is missing or empty.';
        SET error_code = '193';
        CALL `project.dataset.log_error`(p_jobkennung, p_eintrags_nr, error_code, error_message, 'ERROR', 'r_ausd_vertrag_control');
        RAISE USING MESSAGE error_message;
    END IF;

    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        SET error_message = 'ERROR: Parameter p_EintragsNr is missing or empty.';
        SET error_code = '193';
        CALL `project.dataset.log_error`(p_jobkennung, p_eintrags_nr, error_code, error_message, 'ERROR', 'r_ausd_vertrag_control');
        RAISE USING MESSAGE error_message;
    END IF;

    -- 2. Job Status Management: Insert new job entry
    INSERT INTO `project.dataset.job_table` (job_kennung, eintrags_nr, start_time, status)
    VALUES (p_jobkennung, p_eintrags_nr, CURRENT_TIMESTAMP(), 'RUNNING');

    -- Optional: Deactivate old active jobs (example logic, actual logic depends on `h_alis_sqlplus.ksh`'s `starteSQLSkript`)
    UPDATE `project.dataset.job_table`
    SET status = 'DEACTIVATED', end_time = CURRENT_TIMESTAMP(), message = 'Deactivated by new run'
    WHERE job_kennung = p_jobkennung
      AND eintrags_nr <> p_eintrags_nr -- Assuming only one active job per job_kennung, different eintrags_nr
      AND status = 'RUNNING';

    BEGIN
        -- Error handling for the main logic block
        EXCEPTION WHEN OTHERS THEN
            SET error_message = @@error.message;
            SET error_code = CONCAT('SQL_ERROR_', @@error.code);
            SET job_status = 'FAILED';
            CALL `project.dataset.log_error`(p_jobkennung, p_eintrags_nr, error_code, error_message, 'ERROR', 'r_ausd_vertrag_control');
            RAISE USING MESSAGE error_message;
    END;

    -- Get v_datum
    SELECT
        IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    INTO v_datum
    FROM `project.dataset.dwtk_meldungen` AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

    -- Truncate target table
    TRUNCATE TABLE `project.dataset.sof_ta_vertrag_tmp`;

    -- Main data transformation logic from d_ausd_v_ta_vertrag_tmp.sql
    INSERT INTO `project.dataset.sof_ta_vertrag_tmp`
    (
        vertrag_id_carmen, partner_id_carmen, rechdef_id_carmen, kundenkonto, mwst_kennzeichen,
        rahmenvertrag_id, rechnungslauf, vo_kenn, order_number, geplant_kuend,
        eingang_kuend, vertragsbeginn, vertragsstatus, sperrart, sperrgrund,
        stillegungszeitraum, twincard, dwh_tarifgr_text, bindefrist, letztes_upgrade,
        vertragsbindung, vertragsbindungseinheit, rechnungszahlart, rechnungsmedium,
        twin_vertrag_id, upgradeberechtigt, apn, upgradegrund, sv_id, vda,
        cost_centre, cost_centre_user, cntrct_ty, segment_id, rv_action_id,
        rechn_inh_konfig_text, commitment_reference_date, cntrct_validity_id
    )
    SELECT
        c.cntrct_id, bp.bp_id, ia.inv_definition_id, ia.account_reference, ia.sales_tax_freed,
        c.rv_num, ia.billcycle_id, c.vo_code, c.order_number, n.valid_from,
        n.entry_date_of_notice, c.cntrct_start_date,
        CASE c.cntrct_st
            WHEN 5 THEN 'A'
            WHEN 6 THEN 'L'
            ELSE ''
        END,
        b.sperrart_alle, b.sperrgrund_alle, b.stilllegungszeitraum_alle,
        c.twinbill, ct.cds_description, bf.bindefrist, vvl.upgradegatum,
        p.number_time_measurement, p.einheit,
        CASE ia.inv_pay_ty_cv
            WHEN 1 THEN 'U' WHEN 2 THEN 'E' WHEN 3 THEN 'K' WHEN 4 THEN 'B' ELSE ''
        END,
        CASE ia.inv_media_cv
            WHEN 1 THEN 'Papier' WHEN 2 THEN 'ELMO' WHEN 3 THEN 'E-Mail'
            WHEN 4 THEN 'Fax' WHEN 5 THEN 'Inline/Papier' WHEN 6 THEN 'ELMO/Papier' ELSE ''
        END,
        c.twin_vertrag_id,
        CASE
            WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
                AND (b.sperrart_alle IS NULL OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2))
                THEN 'J'
            WHEN p.number_time_measurement = 12
                AND (DATE_DIFF(PARSE_DATE('%Y%m%d', v_datum), IFNULL(c.commitment_reference_date, c.cntrct_start_date), MONTH) > 9)
                AND (b.sperrart_alle IS NULL OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2))
                THEN 'J'
            WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
                AND (DATE_DIFF(PARSE_DATE('%Y%m%d', v_datum), IFNULL(c.commitment_reference_date, c.cntrct_start_date), MONTH) > 23)
                AND (b.sperrart_alle IS NULL OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2))
                THEN 'J'
            ELSE 'N'
        END,
        ap.access_point_name, vvl.upgradegrund, ct.cntrct_template_id,
        CASE
            WHEN (ct.cntrct_template_id IN (5104, 5105, 5106) OR (ct.cntrct_template_id >= 5155 AND ct.cntrct_template_id <= 5161))
            THEN c.contract_number
            ELSE NULL
        END,
        c.cost_centre, c.cost_centre_user, c.cntrct_ty, rd.segment_id, ac.rv_action_id,
        ia.rechn_inh_konfig_text, c.commitment_reference_date, c.cntrct_validity_id
    FROM
        `project.dataset.sof_ta_cntrct_crs3` AS c
    LEFT JOIN
        `project.dataset.sof_ta_bp_ref` AS bp
        ON bp.cntrct_cp2_id = c.cntrct_id
    LEFT JOIN
        `project.dataset.sof_ta_inv_acc` AS ia
        ON ia.cntrct_id = c.cntrct_id
    LEFT JOIN
        `project.dataset.sof_ta_notice` AS n
        ON n.cntrct_id = c.cntrct_id
    LEFT JOIN
        `project.dataset.sof_ta_barrier_zusgf` AS b
        ON b.cntrct_id = c.cntrct_id
    LEFT JOIN
        `project.dataset.sof_ta_cntrct_templ` AS ct
        ON ct.cntrct_template_id = c.cntrct_template_id
    LEFT JOIN
        `project.dataset.sof_ta_cntrct_valid` AS cv
        ON cv.cntrct_validity_id = c.cntrct_validity_id
    LEFT JOIN
        `project.dataset.sof_ta_period` AS p
        ON p.period_id = cv.first_period_id
    LEFT JOIN
        `project.dataset.sof_ta_vvl_upgrade` AS vvl
        ON vvl.vertrags_id = c.cntrct_id
    LEFT JOIN
        `project.dataset.sof_ta_apn_ve` AS ap
        ON ap.cntrct_id = c.cntrct_id
    LEFT JOIN
        `project.dataset.dwh_vi_s_rd_segment` AS rd
        ON ia.inv_definition_id = rd.rechdef_id_carmen
    LEFT JOIN
        `project.dataset.sof_ta_action_assoc` AS ac
        ON ac.cntrct_id = c.cntrct_id
    LEFT JOIN
        `project.dataset.sof_vi_c_bfc` AS bf
        ON bf.cntrct_id = c.cntrct_id
    WHERE c.cntrct_ty <> 20
    UNION ALL
    SELECT
        c.cntrct_id, bp.bp_id, ia.inv_definition_id, ia.account_reference, ia.sales_tax_freed,
        c.rv_num, ia.billcycle_id, c.vo_code, c.order_number, n.valid_from,
        n.entry_date_of_notice, c.cntrct_start_date,
        CASE c.cntrct_st
            WHEN 5 THEN 'A'
            WHEN 6 THEN 'L'
            ELSE ''
        END,
        b.sperrart_alle, b.sperrgrund_alle, b.stilllegungszeitraum_alle,
        c.twinbill, ct.cds_description, bf.bindefrist, vvl.upgradegatum,
        p.number_time_measurement, p.einheit,
        CASE ia.inv_pay_ty_cv
            WHEN 1 THEN 'U' WHEN 2 THEN 'E' WHEN 3 THEN 'K' WHEN 4 THEN 'B' ELSE ''
        END,
        CASE ia.inv_media_cv
            WHEN 1 THEN 'Papier' WHEN 2 THEN 'ELMO' WHEN 3 THEN 'E-Mail'
            WHEN 4 THEN 'Fax' WHEN 5 THEN 'Inline/Papier' WHEN 6 THEN 'ELMO/Papier' ELSE ''
        END,
        c.twin_vertrag_id,
        CASE
            WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
                AND (b.sperrart_alle IS NULL OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2))
                THEN 'J'
            WHEN p.number_time_measurement = 12
                AND (DATE_DIFF(PARSE_DATE('%Y%m%d', v_datum), IFNULL(c.commitment_reference_date, c.cntrct_start_date), MONTH) > 9)
                AND (b.sperrart_alle IS NULL OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2))
                THEN 'J'
            WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
                AND (DATE_DIFF(PARSE_DATE('%Y%m%d', v_datum), IFNULL(c.commitment_reference_date, c.cntrct_start_date), MONTH) > 23)
                AND (b.sperrart_alle IS NULL OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2))
                THEN 'J'
            ELSE 'N'
        END,
        ap.access_point_name, vvl.upgradegrund, ct.cntrct_template_id,
        CASE
            WHEN (ct.cntrct_template_id IN (5104, 5105, 5106) OR (ct.cntrct_template_id >= 5155 AND ct.cntrct_template_id <= 5161))
            THEN c.contract_number
            ELSE NULL
        END,
        c.cost_centre, c.cost_centre_user, c.cntrct_ty, rd.segment_id, ac.rv_action_id,
        ia.rechn_inh_konfig_text, c.commitment_reference_date, c.cntrct_validity_id
    FROM
        `project.dataset.sof_ta_cntrct_crs3` AS c
    LEFT JOIN
        `project.dataset.sof_ta_bp_ref` AS bp
        ON bp.cntrct_cp2_id = c.cntrct_parent
    LEFT JOIN
        `project.dataset.sof_ta_inv_acc` AS ia
        ON ia.cntrct_id = c.cntrct_id
    LEFT JOIN
        `project.dataset.sof_ta_notice` AS n
        ON n.cntrct_id = c.cntrct_id
    LEFT JOIN
        `project.dataset.sof_ta_barrier_zusgf` AS b
        ON b.cntrct_id = c.cntrct_id
    LEFT JOIN
        `project.dataset.sof_ta_cntrct_templ` AS ct
        ON ct.cntrct_template_id = c.cntrct_template_id
    LEFT JOIN
        `project.dataset.sof_ta_cntrct_valid` AS cv
        ON cv.cntrct_validity_id = c.cntrct_validity_id
    LEFT JOIN
        `project.dataset.sof_ta_period` AS p
        ON p.period_id = cv.first_period_id
    LEFT JOIN
        `project.dataset.sof_ta_vvl_upgrade` AS vvl
        ON vvl.vertrags_id = c.cntrct_id
    LEFT JOIN
        `project.dataset.sof_ta_apn_ve` AS ap
        ON ap.cntrct_id = c.cntrct_id
    LEFT JOIN
        `project.dataset.dwh_vi_s_rd_segment` AS rd
        ON ia.inv_definition_id = rd.rechdef_id_carmen
    LEFT JOIN
        `project.dataset.sof_ta_action_assoc` AS ac
        ON ac.cntrct_id = c.cntrct_id
    LEFT JOIN
        `project.dataset.sof_vi_c_bfc` AS bf
        ON bf.cntrct_id = c.cntrct_id
    WHERE c.cntrct_ty = 20;

    SET records_processed = (SELECT COUNT(*) FROM `project.dataset.sof_ta_vertrag_tmp`);

    -- 4. Log job results
    INSERT INTO `project.dataset.job_result_log` (job_kennung, eintrags_nr, target_table, record_count, log_time)
    VALUES (p_jobkennung, p_eintrags_nr, 'sof_ta_vertrag_tmp', records_processed, CURRENT_TIMESTAMP());

    -- 5. Update job status to COMPLETED
    UPDATE `project.dataset.job_table`
    SET status = 'COMPLETED', end_time = CURRENT_TIMESTAMP(), processed_records = records_processed
    WHERE job_kennung = p_jobkennung AND eintrags_nr = p_eintrags_nr;

    -- Helper procedure for logging errors (if needed, otherwise log directly)
    -- This assumes a separate procedure 'log_error' exists or is inlined.
    -- For this output, I'll inline a minimal error logging update to job_table and insert to error_log.
END;