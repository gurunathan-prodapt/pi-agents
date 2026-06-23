-- BigQuery Orchestration Stored Procedure for r_ausd_bp_ta_bpr_instance
-- Replaces the orchestration logic from k_ausd_bp_ta_bpr_instance.ksh.
-- Handles parameter validation, date derivation, and calls the core transformation procedure.
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh

CREATE OR REPLACE PROCEDURE my_gcp_project.my_bq_dataset.r_ausd_bp_ta_bpr_instance(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING,
    IN p_Stichtag STRING, -- Expected format: YYYYMMDD or DDMMYYYY (if parsing logic is complex)
    IN p_wiederanlaufWert STRING,
    OUT records_processed INT64
)
BEGIN
    DECLARE v_stichtag_date DATE;
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_processed_count INT64;

    -- Parameter Validation (simplified for BigQuery procedure)
    IF p_JobKennung IS NULL OR LENGTH(p_JobKennung) = 0 THEN
        RAISE USING MESSAGE = 'Parameter p_JobKennung must not be empty.';
    END IF;

    IF p_EintragsNr IS NULL OR LENGTH(p_EintragsNr) = 0 THEN
        RAISE USING MESSAGE = 'Parameter p_EintragsNr must not be empty.';
    END IF;

    IF p_Stichtag IS NULL OR LENGTH(p_Stichtag) = 0 THEN
        RAISE USING MESSAGE = 'Parameter p_Stichtag must not be empty.';
    END IF;

    -- Date Validation and Parsing (assuming YYYYMMDD for simplicity based on previous SQL content)
    BEGIN
        SET v_stichtag_date = PARSE_DATE('%Y%m%d', p_Stichtag);
    EXCEPTION WHEN ERROR THEN
        RAISE USING MESSAGE = 'Invalid p_Stichtag format. Expected YYYYMMDD. Received: ' || p_Stichtag;
    END;

    -- Date Derivation (replacing gestern.ksh logic)
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

    -- Call the core transformation procedure
    CALL my_gcp_project.my_bq_dataset.d_ausd_bp_ta_bpr_instance_core(
        v_stichtag_date,
        v_processed_count
    );

    SET records_processed = v_processed_count;

    -- Log completion or other actions as needed
    SELECT FORMAT('Procedure r_ausd_bp_ta_bpr_instance completed successfully for Job: %s, Date: %s. Processed records: %d',
                  p_JobKennung, p_Stichtag, records_processed) AS log_message;

END;