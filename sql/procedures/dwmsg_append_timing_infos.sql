-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Target Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

-- This stored procedure appends timing information to the zusatz_infos field in bert_meldung.
-- It attempts to maintain zusatz_infos as a JSON string.
-- Replace `my_project.my_dataset` with your actual BigQuery project and dataset IDs.

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.dwmsg_append_timing_infos`(
    IN p_eintrags_nr STRING,
    IN p_timing_info_key STRING,
    IN p_timing_info_value STRING
)
BEGIN
    DECLARE current_zusatz_infos STRING;
    DECLARE new_zusatz_infos STRING;

    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_eintrags_nr cannot be empty or NULL.';
    END IF;

    SELECT zusatz_infos INTO current_zusatz_infos
    FROM `my_project.my_dataset.bert_meldung`
    WHERE eintrags_nr = p_eintrags_nr;

    IF p_timing_info_key IS NULL OR TRIM(p_timing_info_key) = '' THEN
        -- If key is empty, just append value as general info
        SET new_zusatz_infos = CONCAT(COALESCE(current_zusatz_infos, ''), ' | Timing Info: ', COALESCE(p_timing_info_value, ''));
    ELSE
        -- Initialize or append to JSON string
        IF current_zusatz_infos IS NULL OR TRIM(current_zusatz_infos) = '' THEN
            SET new_zusatz_infos = TO_JSON_STRING(JSON_OBJECT(p_timing_info_key, p_timing_info_value));
        ELSE
            -- Attempt to parse as JSON and update/add, or concatenate if not valid JSON
            BEGIN
                DECLARE temp_json JSON;
                SET temp_json = PARSE_JSON(current_zusatz_infos);
                SET new_zusatz_infos = TO_JSON_STRING(JSON_SET(temp_json, CONCAT('$."', p_timing_info_key, '"'), p_timing_info_value));
            EXCEPTION WHEN ERROR THEN
                -- If not valid JSON, just append as a string
                SET new_zusatz_infos = CONCAT(current_zusatz_infos, ' | ', p_timing_info_key, ': ', COALESCE(p_timing_info_value, ''));
            END;
        END IF;
    END IF;

    UPDATE `my_project.my_dataset.bert_meldung`
    SET
        zusatz_infos = new_zusatz_infos,
        last_update_timestamp = CURRENT_TIMESTAMP()
    WHERE
        eintrags_nr = p_eintrags_nr;
END;