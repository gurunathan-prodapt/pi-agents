-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Description: BigQuery Stored Procedure implementing the core business logic of the original kernel script.

CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_rn_einzeln`(
    IN p_job_id STRING,
    IN p_stichtag STRING, -- DDMMYYYY
    IN p_wiederanlaufwert INT64
)
BEGIN
    DECLARE v_stichtag_date DATE;
    DECLARE v_records_selected INT64 DEFAULT 0;
    DECLARE v_records_deleted INT64 DEFAULT 0;
    DECLARE v_records_inserted INT64 DEFAULT 0;

    -- Log procedure start
    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, component)
    VALUES (p_job_id, CURRENT_TIMESTAMP(), 'INFO', 'k_ausd_bp_ta_rn_einzeln procedure started.', 'kernel');

    -- Convert stichtag to DATE type for comparisons
    SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag);

    -- Intermediate table to hold selected data
    CREATE TEMP TABLE tmp_selected_contracts AS
    SELECT
        dwh_vertrag_id,
        vertrag_nr,
        kunde_id,
        gueltig_von,
        gueltig_bis,
        ladedatum,
        produkt_typ,
        payload
    FROM
        `project.dataset.contract_cache_source`
    WHERE
        DATE(gueltig_von) <= v_stichtag_date
        AND v_stichtag_date < DATE(gueltig_bis)
        AND DATE(ladedatum) < v_stichtag_date
        AND dwh_vertrag_id > p_wiederanlaufwert; -- Filter by restart value

    SET v_records_selected = (SELECT COUNT(1) FROM tmp_selected_contracts);

    -- Log selected records count
    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, component)
    VALUES (p_job_id, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Selected %d records from source.', v_records_selected), 'kernel');

    -- Delete existing records in target based on restart value, if applicable
    -- The original script's behavior regarding deletion is not fully explicit,
    -- but this is a common pattern for restartable loads.
    IF p_wiederanlaufwert > 0 THEN
        DELETE FROM `project.dataset.fos_target_table`
        WHERE dwh_vertrag_id >= p_wiederanlaufwert
        AND processing_job_id = p_job_id; -- Only delete records processed by this job if it's a restart
        SET v_records_deleted = @@row_count;

        INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, component)
        VALUES (p_job_id, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Deleted %d records from target for restart.', v_records_deleted), 'kernel');
    END IF;

    -- Insert processed data into the target table
    INSERT INTO `project.dataset.fos_target_table` (
        dwh_vertrag_id, vertrag_nr, kunde_id, gueltig_von, gueltig_bis,
        ladedatum, produkt_typ, payload, processing_job_id, processing_timestamp
    )
    SELECT
        dwh_vertrag_id, vertrag_nr, kunde_id, gueltig_von, gueltig_bis,
        ladedatum, produkt_typ, payload, p_job_id, CURRENT_TIMESTAMP()
    FROM
        tmp_selected_contracts;

    SET v_records_inserted = @@row_count;

    -- Log inserted records count
    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, component)
    VALUES (p_job_id, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Inserted %d records into target.', v_records_inserted), 'kernel');

    -- Record audit information
    INSERT INTO `project.dataset.processing_audit` (
        audit_id, job_id, stichtag, wiederanlaufwert, source_records_selected,
        target_records_deleted, target_records_inserted, processing_timestamp, component
    )
    VALUES (
        GENERATE_UUID(), p_job_id, v_stichtag_date, p_wiederanlaufwert, v_records_selected,
        v_records_deleted, v_records_inserted, CURRENT_TIMESTAMP(), 'k_ausd_bp_ta_rn_einzeln'
    );

    -- Log procedure end
    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, component)
    VALUES (p_job_id, CURRENT_TIMESTAMP(), 'INFO', 'k_ausd_bp_ta_rn_einzeln procedure finished successfully.', 'kernel');

EXCEPTION WHEN ERROR THEN
    -- Log error details
    INSERT INTO `project.dataset.job_error_log` (
        error_id, job_id, error_timestamp, error_code, error_message, stack_trace, component
    )
    VALUES (
        CAST(GENERATE_UUID() AS STRING), p_job_id, CURRENT_TIMESTAMP(), ERROR_CODE(), ERROR_MESSAGE(),
        (SELECT CONCAT(stack_trace, '\n', @@script.stack_trace) FROM UNNEST(SPLIT(STACK_TRACE(), '\n')) AS stack_trace WHERE STARTS_WITH(stack_trace, '  ')), -- Simplified stack trace capture
        'k_ausd_bp_ta_rn_einzeln'
    );
    -- Re-raise the error to be caught by the calling procedure
    RAISE;
END;