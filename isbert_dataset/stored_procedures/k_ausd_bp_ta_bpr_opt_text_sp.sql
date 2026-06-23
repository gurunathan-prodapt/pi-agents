--
-- BigQuery Stored Procedure, replacing KornShell script k_ausd_bp_ta_bpr_opt_text.ksh
-- and Oracle SQL script d_ausd_bp_ta_bpr_opt_text.sql.
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh
--

CREATE OR REPLACE PROCEDURE `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`(
    p_job_kennung       STRING,
    p_eintrags_nr       STRING,
    p_stichtag          STRING,    -- Expected format: YYYYMMDD
    p_wiederanlauf_wert INT64 DEFAULT 0
)
BEGIN
    DECLARE v_datum_heute         DATE;
    DECLARE v_datum_gestern       DATE;
    DECLARE v_datum               STRING;
    DECLARE v_records_processed   INT64;
    DECLARE v_start_timestamp     TIMESTAMP;
    DECLARE v_end_timestamp       TIMESTAMP;
    DECLARE v_stichtag_date       DATE;

    SET v_start_timestamp = CURRENT_TIMESTAMP();

    -- Parameter validation
    IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
        RAISE USING MESSAGE = 'Parameter p_job_kennung must be provided.';
    END IF;

    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        RAISE USING MESSAGE = 'Parameter p_eintrags_nr must be provided.';
    END IF;

    IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
        RAISE USING MESSAGE = 'Parameter p_stichtag must be provided.';
    END IF;

    SET v_stichtag_date = SAFE.PARSE_DATE('%Y%m%d', p_stichtag);
    IF v_stichtag_date IS NULL THEN
        RAISE USING MESSAGE = FORMAT('Invalid format for p_stichtag. Expected YYYYMMDD, got %s', p_stichtag);
    END IF;

    -- Date derivation (replacing gestern.ksh logic)
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- Derive v_datum (from Oracle SQL script)
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    INTO v_datum
    FROM `isbert_dataset.dwtk_meldungen` AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

    -- Core SQL logic: Truncate and Insert
    -- (Replacing isbert_schema.DWPA_UTIL_SKRIPT.runstatement and INSERT statement)
    TRUNCATE TABLE `isbert_dataset.sof_ta_bpr_opt_text`;

    INSERT INTO `isbert_dataset.sof_ta_bpr_opt_text`
    (
        CNTRCT_ID,
        BPR_ID,
        PDS_DESCRIPTION
    )
    SELECT
        bp.cntrct_id,
        bp.bpr_id,
        bs.pds_description
    FROM
        `isbert_dataset.sof_ta_bpr_optionen` AS bp,
        `isbert_dataset.sof_ta_bpr_beschr` AS bs
    WHERE
        bp.bpr_id = bs.bpr_id;

    SET v_records_processed = @@row_count;

    SET v_end_timestamp = CURRENT_TIMESTAMP();

    -- Logging to a control table (placeholder for job_run_control)
    -- This table schema is inferred based on common logging requirements.
    -- In a real scenario, this DDL would be explicitly provided or generated.
    CREATE TABLE IF NOT EXISTS `isbert_dataset.job_run_control`
    (
        job_id            STRING,
        run_date          DATE,
        stichtag          DATE,
        records_processed INT64,
        status            STRING,
        start_timestamp   TIMESTAMP,
        end_timestamp     TIMESTAMP
    );

    INSERT INTO `isbert_dataset.job_run_control`
    (
        job_id,
        run_date,
        stichtag,
        records_processed,
        status,
        start_timestamp,
        end_timestamp
    )
    VALUES
    (
        p_job_kennung,
        v_datum_heute,
        v_stichtag_date,
        v_records_processed,
        'SUCCESS',
        v_start_timestamp,
        v_end_timestamp
    );

END;