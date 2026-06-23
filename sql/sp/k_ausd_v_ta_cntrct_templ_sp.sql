-- BigQuery Stored Procedure equivalent of k_ausd_v_ta_cntrct_templ.ksh
-- This procedure orchestrates the data preparation process for ta_cntrct_templ.
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING
)
BEGIN
    DECLARE v_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_records_processed INT66 DEFAULT 0;
    DECLARE v_error_message STRING;
    DECLARE v_temp_sql_script STRING;

    -- Define target table name (corresponds to v_TabName in ksh)
    DECLARE v_target_table_name STRING DEFAULT 'ta_cntrct_templ';

    -- Error handling block
    BEGIN
        -- Parameter validation
        IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
            RAISE USING MESSAGE 'FEHLER: JobKennung (j) ist ein notwendiges Argument.';
        END IF;

        IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
            RAISE USING MESSAGE 'FEHLER: EintragsNr (f) ist ein notwendiges Argument.';
        END IF;

        -- Log job start
        INSERT INTO `your_project_id.your_dataset_id.job_audit`
            (job_id, entry_number, start_time, status)
        VALUES
            (p_JobKennung, p_EintragsNr, v_start_time, 'RUNNING');

        -- Get the SQL script content to execute
        -- In a real scenario, this would be read from a dedicated BQ script file
        -- or a configuration table. For this example, we inline it or simulate its execution.
        SET v_temp_sql_script = '''
            -- Declare a variable for the derived date
            DECLARE v_datum_string STRING;
            DECLARE v_current_date DATE;

            -- Determine the processing date (v_datum)
            SELECT
                COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
            FROM
                `your_project_id.your_dataset_id.dwtk_meldungen` AS m
            WHERE
                m.job_kennung = 'BERT_DROP_TEMP_TABLE'
            INTO
                v_datum_string;

            SET v_current_date = PARSE_DATE('%Y%m%d', v_datum_string);

            -- Truncate the target table before insertion
            TRUNCATE TABLE `your_project_id.your_dataset_id.ta_cntrct_templ`;

            -- Insert data into the target table
            INSERT INTO `your_project_id.your_dataset_id.ta_cntrct_templ`
            (cntrct_template_id, cds_description_id, cds_description)
            SELECT
                ct.cntrct_template_id,
                ct.cds_description_id,
                cd.cds_description
            FROM
                `your_project_id.your_dataset_id.cds_ta_cntrct_template` AS ct
            JOIN
                `your_project_id.your_dataset_id.cds_ta_care_description` AS cd
            ON
                ct.cds_description_id = cd.cds_description_id
            WHERE
                ct.insert_at <= v_current_date
            AND
                (ct.modified_at IS NULL OR ct.modified_at > v_current_date)
            AND
                ct.valid_from <= v_current_date
            AND
                (ct.valid_to IS NULL OR ct.valid_to > v_current_date)
            AND
                ct.is_production = 1
            AND
                cd.language = 1;
        ''';

        -- Execute the core SQL transformation script
        EXECUTE IMMEDIATE v_temp_sql_script;

        -- Get the number of processed records
        SELECT COUNT(*)
        FROM `your_project_id.your_dataset_id.ta_cntrct_templ`
        INTO v_records_processed;

        SET v_status = 'SUCCESS';
        SET v_end_time = CURRENT_TIMESTAMP();

        -- Log job success
        UPDATE `your_project_id.your_dataset_id.job_audit`
        SET
            end_time = v_end_time,
            status = v_status,
            records_processed = v_records_processed
        WHERE
            job_id = p_JobKennung AND entry_number = p_EintragsNr AND status = 'RUNNING';

        SELECT '---------- ENDE Datenverarbeitung ----------';
        SELECT CONCAT('Records Processed: ', v_records_processed);

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_error_message = @@error.message;

        -- Log job failure
        UPDATE `your_project_id.your_dataset_id.job_audit`
        SET
            end_time = v_end_time,
            status = v_status,
            error_message = v_error_message
        WHERE
            job_id = p_JobKennung AND entry_number = p_EintragsNr AND status = 'RUNNING';

        RAISE USING MESSAGE CONCAT('Job failed: ', v_error_message);
    END;
END;