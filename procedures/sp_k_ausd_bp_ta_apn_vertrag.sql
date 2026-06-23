-- BigQuery Stored Procedure for Orchestration
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE OR REPLACE PROCEDURE project.dataset.sp_k_ausd_bp_ta_apn_vertrag(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING,
    IN p_Stichtag STRING, -- Input as STRING, will be parsed to DATE
    IN p_wiederanlaufWert STRING DEFAULT '0'
)
BEGIN
    DECLARE v_stichtag_date DATE;
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_record_count INT64;
    DECLARE v_job_name STRING DEFAULT 'k_ausd_bp_ta_apn_vertrag';

    -- Initialize job tracking status
    INSERT INTO project.dataset.job_tracking (job_name, status, stichtag, eintragsnr, details)
    VALUES (
        v_job_name,
        'STARTED',
        SAFE.PARSE_DATE('%d%m%Y', p_Stichtag),
        p_EintragsNr,
        JSON_OBJECT('job_kennung', p_JobKennung, 'wiederanlauf_wert', p_wiederanlaufWert)
    );

    -- Parameter Validation (replacing pruefeParameterGesetzt from h_alis_parameter.ksh)
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        INSERT INTO project.dataset.error_log (job_name, error_code, error_message, severity)
        VALUES (v_job_name, '193', 'Jobkennung parameter is missing or empty.', 'ERROR');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Jobkennung parameter is missing or empty.';
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        INSERT INTO project.dataset.error_log (job_name, error_code, error_message, severity)
        VALUES (v_job_name, '193', 'EintragsNr parameter is missing or empty.', 'ERROR');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'EintragsNr parameter is missing or empty.';
    END IF;

    IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
        INSERT INTO project.dataset.error_log (job_name, error_code, error_message, severity)
        VALUES (v_job_name, '193', 'Stichtag parameter is missing or empty.', 'ERROR');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Stichtag parameter is missing or empty.';
    END IF;

    -- Date Validation (replacing DWDate_Datum_Check from h_alis_date.ksh)
    SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
    IF v_stichtag_date IS NULL THEN
        INSERT INTO project.dataset.error_log (job_name, error_code, error_message, severity)
        VALUES (v_job_name, 'DATE_FORMAT_ERROR', 'Stichtag parameter has an invalid date format (expected DDMMYYYY).', 'ERROR');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Stichtag parameter has an invalid date format.';
    END IF;

    -- Date Derivation (replacing gestern.ksh)
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- Log derived dates (optional)
    -- INSERT INTO project.dataset.error_log (job_name, error_message, severity, details)
    -- VALUES (v_job_name, 'Dates derived', 'INFO', JSON_OBJECT('heute', CAST(v_datum_heute AS STRING), 'gestern', CAST(v_datum_gestern AS STRING)));

    -- Call Core Data Logic Stored Procedure
    CALL project.dataset.sp_d_ausd_bp_ta_apn_vertrag(v_stichtag_date);

    -- Record Count (replacing eval "v_records=`cat $tmpFile`")
    SELECT COUNT(*)
    INTO v_record_count
    FROM project.dataset.SOF_TA_APN_VERTRAG
    WHERE processing_stichtag = v_stichtag_date; -- Filter by the stichtag if applicable

    -- Update Job Tracking
    UPDATE project.dataset.job_tracking
    SET
        status = 'SUCCESS',
        record_count = v_record_count,
        track_timestamp = CURRENT_TIMESTAMP(),
        details = JSON_OBJECT('job_kennung', p_JobKennung, 'wiederanlauf_wert', p_wiederanlaufWert, 'message', 'Job completed successfully.')
    WHERE job_name = v_job_name
      AND stichtag = v_stichtag_date
      AND eintragsnr = p_EintragsNr
      AND status = 'STARTED'; -- Ensure we update the correct "STARTED" entry

EXCEPTION WHEN ERROR THEN
    -- Log the error and update job tracking to FAILED
    INSERT INTO project.dataset.error_log (job_name, error_code, error_message, severity)
    VALUES (
        v_job_name,
        CAST(BQ.RAISE_ERROR() AS STRING), -- Captures the error message
        'Orchestration procedure failed',
        'CRITICAL'
    );
    -- Update job tracking status to FAILED
    UPDATE project.dataset.job_tracking
    SET
        status = 'FAILED',
        track_timestamp = CURRENT_TIMESTAMP(),
        details = JSON_OBJECT('error_message', BQ.RAISE_ERROR())
    WHERE job_name = v_job_name
      AND stichtag = v_stichtag_date
      AND eintragsnr = p_EintragsNr
      AND status = 'STARTED'; -- Update the corresponding "STARTED" entry

    RAISE; -- Re-raise the error to signal failure to the caller
END;