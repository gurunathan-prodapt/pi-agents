-- BigQuery Stored Procedure for contract validity data processing
-- Replaces logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
-- and vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_valid.sql

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag`(
    p_JobKennung STRING,
    p_EintragsNr STRING
)
BEGIN
    DECLARE v_datum DATE;
    DECLARE v_records INT64 DEFAULT 0;

    -- 1. Determine the cutoff date (v_datum)
    -- This simulates the logic deriving v_datum from isbert_schema.dwtk_meldungen
    -- based on the latest timecreated for job_kennung = 'BERT_DROP_TEMP_TABLE'.
    SELECT
        MAX(DATE(timecreated))
    INTO
        v_datum
    FROM
        `project.isbert_schema.dwtk_meldungen`
    WHERE
        job_kennung = 'BERT_DROP_TEMP_TABLE';

    -- Handle case where v_datum might not be found
    IF v_datum IS NULL THEN
        RAISE USING MESSAGE = 'Cutoff date (v_datum) could not be determined from `project.isbert_schema.dwtk_meldungen`. Aborting procedure.';
    END IF;

    -- 2. Truncate the target table
    TRUNCATE TABLE `project.dataset.sof_ta_cntrct_valid`;

    -- 3. Insert data from source with specified filtering and column mapping
    INSERT INTO `project.dataset.sof_ta_cntrct_valid` (
        cntrct_validity_id,
        first_period_id,
        following_period_id,
        first_notice_period_id,
        follow_notice_period_id,
        bfc_age -- This maps from the source's insert_at
    )
    SELECT
        cv.cntrct_validity_id,
        cv.first_period_id,
        cv.following_period_id,
        cv.first_notice_period_id,
        cv.follow_notice_period_id,
        cv.insert_at -- The original column is insert_at, which becomes bfc_age
    FROM
        `project.source_dataset.cds_ta_cntrct_validity` AS cv
    WHERE
        DATE(cv.insert_at) <= v_datum
        AND (cv.modified_at IS NULL OR DATE(cv.modified_at) > v_datum);

    -- 4. Get the count of loaded records
    SET v_records = (SELECT COUNT(*) FROM `project.dataset.sof_ta_cntrct_valid`);

    -- 5. Optional: Insert into job_audit_log for auditing purposes
    INSERT INTO `project.dataset.job_audit_log` (
        job_kennung,
        entry_number,
        run_timestamp,
        records_loaded,
        status,
        message
    )
    VALUES (
        p_JobKennung,
        p_EintragsNr,
        CURRENT_DATETIME(),
        v_records,
        'SUCCESS',
        'Data loaded successfully into sof_ta_cntrct_valid.'
    );

    -- Optional: Log success message to BigQuery logs
    SELECT FORMAT('Procedure `project.dataset.r_ausd_vertrag` completed successfully. Loaded %d records.', v_records);

EXCEPTION WHEN ERROR THEN
    -- Optional: Log error message to job_audit_log
    INSERT INTO `project.dataset.job_audit_log` (
        job_kennung,
        entry_number,
        run_timestamp,
        records_loaded,
        status,
        message
    )
    VALUES (
        p_JobKennung,
        p_EintragsNr,
        CURRENT_DATETIME(),
        v_records, -- Records loaded before error (could be 0)
        'FAILED',
        ERROR_MESSAGE()
    );
    RAISE; -- Re-raise the error to propagate it
END;