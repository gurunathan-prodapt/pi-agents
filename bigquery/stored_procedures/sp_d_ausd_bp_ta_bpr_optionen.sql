-- BigQuery Stored Procedure for k_ausd_bp_ta_bpr_optionen.ksh
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_optionen.ksh

CREATE OR REPLACE PROCEDURE `your-gcp-project-id.your-dataset.sp_d_ausd_bp_ta_bpr_optionen`(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_Stichtag STRING, -- Expected format 'DDMMYYYY'
    p_wiederanlaufWert STRING
)
OPTIONS(
    description="Migrated logic for preparing and populating base product tariff options."
)
BEGIN
    -- Declare variables
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_datum STRING;
    DECLARE v_records INT64;

    -- Set current dates
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- Parameter Validation
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        INSERT INTO `your-gcp-project-id.your-dataset.error_log` (job_name, error_nr, error_arg, created_at)
        VALUES ('sp_d_ausd_bp_ta_bpr_optionen', 1001, 'p_JobKennung cannot be NULL or empty', CURRENT_TIMESTAMP());
        RAISE USING MESSAGE 'Error: p_JobKennung cannot be NULL or empty.';
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        INSERT INTO `your-gcp-project-id.your-dataset.error_log` (job_name, error_nr, error_arg, created_at)
        VALUES ('sp_d_ausd_bp_ta_bpr_optionen', 1002, 'p_EintragsNr cannot be NULL or empty', CURRENT_TIMESTAMP());
        RAISE USING MESSAGE 'Error: p_EintragsNr cannot be NULL or empty.';
    END IF;

    IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
        INSERT INTO `your-gcp-project-id.your-dataset.error_log` (job_name, error_nr, error_arg, created_at)
        VALUES ('sp_d_ausd_bp_ta_bpr_optionen', 1003, 'p_Stichtag cannot be NULL or empty', CURRENT_TIMESTAMP());
        RAISE USING MESSAGE 'Error: p_Stichtag cannot be NULL or empty.';
    END IF;

    IF SAFE.PARSE_DATE('%d%m%Y', p_Stichtag) IS NULL THEN
        INSERT INTO `your-gcp-project-id.your-dataset.error_log` (job_name, error_nr, error_arg, created_at)
        VALUES ('sp_d_ausd_bp_ta_bpr_optionen', 1004, 'Invalid date format for p_Stichtag. Expected DDMMYYYY.', CURRENT_TIMESTAMP());
        RAISE USING MESSAGE 'Error: Invalid date format for p_Stichtag. Expected DDMMYYYY.';
    END IF;

    -- Derive v_datum from dwtk_meldungen table
    -- This logic mirrors the original script's use of a MAX(timecreated) for a specific job_kennung
    SET v_datum = (
        SELECT IFNULL(MAX(FORMAT_DATE('%Y%m%d', DATE(timecreated))), '19000101')
        FROM `your-gcp-project-id.your-isbert-schema-dataset.dwtk_meldungen`
        WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    -- Truncate the target table
    TRUNCATE TABLE `your-gcp-project-id.your-dataset.sof_ta_bpr_optionen`;

    -- Insert data from source to target table
    INSERT INTO `your-gcp-project-id.your-dataset.sof_ta_bpr_optionen` (cntrct_id, bpr_id)
    SELECT
        bp.cntrct_id,
        bp.bpr_id
    FROM `your-gcp-project-id.your-dataset.sof_ta_bpr_instance` AS bp;

    -- Get the number of records processed
    SET v_records = (SELECT COUNT(*) FROM `your-gcp-project-id.your-dataset.sof_ta_bpr_optionen`);

    -- Log job details
    INSERT INTO `your-gcp-project-id.your-dataset.job_log` (tab_name, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert, records, created_at)
    VALUES (
        'sof_ta_bpr_optionen',
        p_JobKennung,
        p_EintragsNr,
        p_Stichtag,
        p_wiederanlaufWert,
        v_records,
        CURRENT_TIMESTAMP()
    );

    -- Optional: Return success message or status
    -- SELECT 'Stored procedure sp_d_ausd_bp_ta_bpr_optionen executed successfully.' AS status;

END;