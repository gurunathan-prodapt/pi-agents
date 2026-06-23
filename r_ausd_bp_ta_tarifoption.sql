-- Migrated from vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
-- BigQuery orchestration stored procedure for k_ausd_bp_ta_tarifoption.ksh.
-- This procedure handles parameter validation, date derivation, calls the core business logic,
-- and performs logging.

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.r_ausd_bp_ta_tarifoption`(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_Stichtag STRING, -- Expected DDMMYYYY format
    p_wiederanlaufWert STRING
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'k_ausd_bp_ta_tarifoption';
    DECLARE v_run_id STRING;
    DECLARE v_stichtag_date DATE;
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_record_count INT64;
    DECLARE v_start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    -- Generate a unique run_id using a combination of entry number and timestamp
    SET v_run_id = CONCAT(p_EintragsNr, '_', FORMAT_TIMESTAMP('%Y%m%d%H%M%S', v_start_timestamp));

    BEGIN
        -- Log job start
        INSERT INTO `your_project_id.your_dataset_id.job_log` (job_id, run_id, start_timestamp, status, message)
        VALUES (v_job_name, v_run_id, v_start_timestamp, 'RUNNING', 'Job started');

        -- 1. Parameter Validation
        IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
            RAISE USING MESSAGE = 'Parameter p_JobKennung cannot be NULL or empty.';
        END IF;

        IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
            RAISE USING MESSAGE = 'Parameter p_EintragsNr cannot be NULL or empty.';
        END IF;

        IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
            RAISE USING MESSAGE = 'Parameter p_Stichtag cannot be NULL or empty.';
        END IF;

        -- Validate p_Stichtag format (DDMMYYYY)
        SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
        IF v_stichtag_date IS NULL THEN
            RAISE USING MESSAGE = FORMAT('Parameter p_Stichtag "%s" is not in DDMMYYYY format.', p_Stichtag);
        END IF;

        -- 2. Date Derivation (replacing h_alis_date.ksh and gestern.ksh logic)
        SET v_datum_heute = CURRENT_DATE();
        SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

        -- 3. Call Core Business Logic Stored Procedure
        -- (Replacing the `starteSQLSkript` and `d_ausd_bp_ta_tarifoption.sql` execution)
        CALL `your_project_id.your_dataset_id.d_ausd_bp_ta_tarifoption_core`(
            p_EintragsNr,
            p_JobKennung,
            v_stichtag_date,
            v_datum_heute,
            v_datum_gestern,
            p_wiederanlaufWert,
            OUT v_record_count
        );

        -- Log job success
        INSERT INTO `your_project_id.your_dataset_id.job_log` (job_id, run_id, start_timestamp, end_timestamp, status, record_count, message)
        VALUES (v_job_name, v_run_id, v_start_timestamp, CURRENT_TIMESTAMP(), 'SUCCEEDED', v_record_count, 'Job completed successfully');

    EXCEPTION WHEN ERROR THEN
        -- Log job failure (replacing f_alis_msgerr.ksh and DWMSG_MeldeFehler functionality)
        INSERT INTO `your_project_id.your_dataset_id.error_log` (job_id, run_id, error_timestamp, error_message, stack_trace)
        VALUES (v_job_name, v_run_id, CURRENT_TIMESTAMP(), ERROR_MESSAGE(), @@error.stack_trace);

        INSERT INTO `your_project_id.your_dataset_id.job_log` (job_id, run_id, start_timestamp, end_timestamp, status, message)
        VALUES (v_job_name, v_run_id, v_start_timestamp, CURRENT_TIMESTAMP(), 'FAILED', ERROR_MESSAGE());

        RAISE; -- Re-raise the error to propagate it further if called by an orchestrator
    END;
END;