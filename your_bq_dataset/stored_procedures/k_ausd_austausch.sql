-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.k_ausd_austausch`(
    p_jobkennung STRING,
    p_stichtag STRING, -- DDMMYYYY
    p_eintragsnr STRING,
    p_wiederanlaufWert INT64
)
BEGIN
    DECLARE v_stichtag_date DATE;
    DECLARE v_job_id STRING;
    DECLARE v_message STRING;

    SET v_job_id = GENERATE_UUID();

    -- Convert p_stichtag to DATE
    SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag);

    -- Log start of the core data preparation
    INSERT INTO `your_gcp_project.your_bq_dataset.job_log` (
        job_id, job_name, start_time, status, message, stichtag_param, wiederanlaufwert_param
    ) VALUES (
        v_job_id, 'k_ausd_austausch', CURRENT_TIMESTAMP(), 'RUNNING', 'Starting core data preparation', p_stichtag, p_wiederanlaufWert
    );

    BEGIN TRANSACTION;

    BEGIN
        -- Delete existing entries >= restart threshold before insert/reload
        DELETE FROM `your_gcp_project.your_bq_dataset.fos_table`
        WHERE dwh_vertrag_id >= p_wiederanlaufWert;

        -- Insert filtered snapshot into target table
        INSERT INTO `your_gcp_project.your_bq_dataset.fos_table` (
            dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum,
            some_data_column_1, some_data_column_2, some_data_column_3 -- Placeholder columns, adjust as per actual schema
        )
        SELECT
            dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum,
            some_data_column_1, some_data_column_2, some_data_column_3 -- Placeholder columns, adjust as per actual schema
        FROM `your_gcp_project.your_bq_dataset.contract_cache` -- Hypothesized source table
        WHERE
            gueltig_von <= v_stichtag_date
            AND v_stichtag_date < gueltig_bis
            AND ladedatum < v_stichtag_date
            AND dwh_vertrag_id > p_wiederanlaufWert;

        COMMIT TRANSACTION;

        -- Log success
        INSERT INTO `your_gcp_project.your_bq_dataset.job_log` (
            job_id, job_name, end_time, status, message
        ) VALUES (
            v_job_id, 'k_ausd_austausch', CURRENT_TIMESTAMP(), 'SUCCESS', 'Core data preparation completed successfully'
        );

    EXCEPTION WHEN ERROR THEN
        SET v_message = @@error.message;
        ROLLBACK TRANSACTION;

        -- Log error
        INSERT INTO `your_gcp_project.your_bq_dataset.job_log` (
            job_id, job_name, end_time, status, message
        ) VALUES (
            v_job_id, 'k_ausd_austausch', CURRENT_TIMESTAMP(), 'FAILED', v_message
        );
        RAISE; -- Re-raise the error to propagate
    END;
END;