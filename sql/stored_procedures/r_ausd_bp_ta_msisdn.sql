-- BigQuery Stored Procedure: project.dataset.r_ausd_bp_ta_msisdn
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
-- Orchestrates the data preparation process for MSISDN basis product data.

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.r_ausd_bp_ta_msisdn`(
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING,
    IN p_stichtag STRING, -- Expected format 'DDMMYYYY'
    IN p_wiederanlauf_wert STRING -- Not explicitly used in this migration, but kept for signature.
)
BEGIN
    DECLARE v_stichtag_date DATE;
    DECLARE v_records_processed INT64 DEFAULT 0;
    DECLARE v_error_message STRING;
    DECLARE v_start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_status STRING DEFAULT 'FAILED';

    -- Derive today's and yesterday's dates (replaces gestern.ksh)
    DECLARE v_today_date DATE DEFAULT CURRENT_DATE();
    DECLARE v_yesterday_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- Error Handling Block
    BEGIN
        -- 1. Validate p_stichtag format (replaces DWDate_Datum_Check)
        BEGIN
            SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag);
        EXCEPTION WHEN ERROR THEN
            SET v_error_message = 'Invalid p_stichtag format. Expected DDMMYYYY. Got: ' || p_stichtag;
            RAISE USING MESSAGE v_error_message;
        END;

        -- 2. Call the core transformation stored procedure
        CALL `your_gcp_project.your_bq_dataset.d_ausd_bp_ta_msisdn`(v_stichtag_date);

        -- 3. Capture record count (replaces reading from tmp file)
        SELECT COUNT(*)
        INTO v_records_processed
        FROM `your_gcp_project.your_bq_dataset.PoolBasisprodukt`
        WHERE _processing_date = CURRENT_DATE(); -- Assuming data is partitioned by _processing_date (load date)

        SET v_status = 'SUCCESS';

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        -- Log error details
        INSERT INTO `your_gcp_project.your_bq_dataset.job_error_log` (
            job_kennung, eintrags_nr, stichtag, error_message, error_timestamp
        )
        VALUES (
            p_job_kennung, p_eintrags_nr, v_stichtag_date, v_error_message, CURRENT_TIMESTAMP()
        );
        -- Re-raise the error to propagate it to the caller (e.g., Airflow)
        RAISE;

    FINALLY
        SET v_end_timestamp = CURRENT_TIMESTAMP();
        -- Log audit information (replaces FOSJobErzeugeEintrag)
        INSERT INTO `your_gcp_project.your_bq_dataset.job_audit_log` (
            job_kennung, eintrags_nr, stichtag, records_processed, start_timestamp, end_timestamp, status
        )
        VALUES (
            p_job_kennung, p_eintrags_nr, v_stichtag_date, v_records_processed, v_start_timestamp, v_end_timestamp, v_status
        );
    END;
END;