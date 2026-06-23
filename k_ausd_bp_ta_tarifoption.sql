-- Legacy Source: vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
-- Job: vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
-- Description: BigQuery Stored Procedure to migrate the ksh script and its core SQL logic.

CREATE OR REPLACE PROCEDURE `your_project.your_dataset.k_ausd_bp_ta_tarifoption`(
    job_kennung STRING,
    entry_nr STRING,
    as_of_date_str STRING,
    restart_val INT64
)
OPTIONS(
  description="Migrated stored procedure for k_ausd_bp_ta_tarifoption.ksh. Processes tariff options data."
)
BEGIN
    -- Declare variables
    DECLARE v_tab_name STRING DEFAULT 'PoolBasisprodukt';
    DECLARE v_stichtag_date DATE;
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_records INT64;
    DECLARE v_error_message STRING;
    DECLARE v_datum_suffix STRING; -- For dynamic table name like sof$ta_bpr_opt_text_&v_datum

    -- Error handling block
    BEGIN
        -- 1. Parameter Validation
        IF job_kennung IS NULL OR job_kennung = '' THEN
            RAISE USING MESSAGE = 'Parameter "Jobkennung" (job_kennung) must be set.';
        END IF;

        IF entry_nr IS NULL OR entry_nr = '' THEN
            RAISE USING MESSAGE = 'Parameter "EintragsNr" (entry_nr) must be set.';
        END IF;

        IF as_of_date_str IS NULL OR as_of_date_str = '' THEN
            RAISE USING MESSAGE = 'Parameter "Stichtag" (as_of_date_str) must be set.';
        END IF;

        -- Date Format Validation (DDMMYYYY)
        SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', as_of_date_str);
        IF v_stichtag_date IS NULL THEN
            RAISE USING MESSAGE = FORMAT('Parameter "Stichtag" (%s) has an invalid date format. Expected DDMMYYYY.', as_of_date_str);
        END IF;

        -- Default restart_val if not provided (KornShell: if [[ -z "$p_wiederanlaufWert" ]])
        IF restart_val IS NULL THEN
            SET restart_val = 0;
        END IF;

        -- Date Derivation (replacing gestern.ksh logic)
        SET v_datum_heute = CURRENT_DATE();
        SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

        -- Derive v_datum_suffix from dwtk_meldungen (replacing SQL*Plus COLUMN new_value v_datum logic)
        SET v_datum_suffix = (
            SELECT FORMAT_DATE('%Y%m%d', MAX(t2.timecreated))
            FROM `your_project.your_dataset.dwtk_meldungen` t2
            WHERE t2.job_kennung = 'BERT_DROP_TEMP_TABLE'
        );
        IF v_datum_suffix IS NULL THEN
            SET v_datum_suffix = '19000101'; -- Default value if no record found
        END IF;

        -- Core SQL Logic from d_ausd_bp_ta_tarifoption.sql
        -- Clear the target table for idempotency (similar to Oracle DROP/CREATE)
        TRUNCATE TABLE `your_project.your_dataset.sof_ta_tarifoption`;

        -- Step01 & Step9: Build intermediate and final tables
        -- This part translates the Oracle SQL logic, aggregating options by category.

        INSERT INTO `your_project.your_dataset.sof_ta_tarifoption` (
            cntrct_id,
            business_option,
            sonstige_option,
            gprs_option
        )
        WITH
        -- sof$ta_bpr_opt_filter equivalent: join base tables to categorize options
        sof_ta_bpr_opt_filter_cte AS (
            SELECT
                t.bpr_id,
                t.cntrct_id,
                t.pds_description,
                l.opt_kategorie
            FROM
                `your_project.your_dataset.sof_ta_l_bpr_optionen_filter` AS l
            INNER JOIN
                -- Assuming sof_ta_bpr_opt_text contains relevant data or is a view handling the dynamic date part.
                `your_project.your_dataset.sof_ta_bpr_opt_text` AS t
                ON t.bpr_id = l.bpr_id
        ),
        -- Group by contract ID and aggregate descriptions for each category
        aggregated_options AS (
            SELECT
                cntrct_id,
                -- STRING_AGG concatenates non-NULL strings, ordered to ensure consistent output.
                STRING_AGG(CASE WHEN opt_kategorie = 'BUDGET' THEN pds_description END, ', ' ORDER BY pds_description) AS raw_business_option,
                STRING_AGG(CASE WHEN opt_kategorie = 'SONST' THEN pds_description END, ', ' ORDER BY pds_description) AS raw_sonstige_option,
                STRING_AGG(CASE WHEN opt_kategorie = 'GPRS' THEN pds_description END, ', ' ORDER BY pds_description) AS raw_gprs_option
            FROM
                sof_ta_bpr_opt_filter_cte
            GROUP BY
                cntrct_id
        )
        SELECT
            cntrct_id,
            -- Apply the trimming and substring logic found in the original SQL
            -- TRIM(LEADING ', ' FROM ...) removes a leading comma-space if present (from STRING_AGG)
            -- SAFE_SUBSTR ensures string truncation does not error if length is less than 500
            SAFE_SUBSTR(TRIM(LEADING ', ' FROM COALESCE(raw_business_option, '')), 1, 500) AS business_option,
            SAFE_SUBSTR(TRIM(LEADING ', ' FROM COALESCE(raw_sonstige_option, '')), 1, 500) AS sonstige_option,
            SAFE_SUBSTR(TRIM(LEADING ', ' FROM COALESCE(raw_gprs_option, '')), 1, 500) AS gprs_option
        FROM
            aggregated_options;

        -- Capture records processed
        SET v_records = (SELECT COUNT(*) FROM `your_project.your_dataset.sof_ta_tarifoption`);

        -- Log job success in audit table
        INSERT INTO `your_project.your_dataset.job_audit_table` (
            job_identifier,
            entry_number,
            as_of_date,
            restart_value,
            start_timestamp,
            end_timestamp,
            status,
            records_processed
        ) VALUES (
            job_kennung,
            entry_nr,
            v_stichtag_date,
            restart_val,
            CURRENT_TIMESTAMP(), -- start_timestamp is recorded here as the procedure begins
            CURRENT_TIMESTAMP(),
            'SUCCESS',
            v_records
        );

    EXCEPTION WHEN ERROR THEN
        -- Capture error message
        SET v_error_message = @@error.message;

        -- Log job failure in audit table
        INSERT INTO `your_project.your_dataset.job_audit_table` (
            job_identifier,
            entry_number,
            as_of_date,
            restart_value,
            start_timestamp,
            end_timestamp,
            status,
            error_message
        ) VALUES (
            job_kennung,
            entry_nr,
            v_stichtag_date,
            restart_val,
            CURRENT_TIMESTAMP(), -- start_timestamp is recorded here as the procedure begins
            CURRENT_TIMESTAMP(),
            'FAILED',
            v_error_message
        );
        -- Re-raise the error to signal failure to the caller
        RAISE;
    END;
END;