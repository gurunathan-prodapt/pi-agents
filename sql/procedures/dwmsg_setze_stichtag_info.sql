-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Target Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

-- This stored procedure updates the zusatz_infos field in bert_meldung with stichtag (key date) information.
-- Replace `my_project.my_dataset` with your actual BigQuery project and dataset IDs.

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.dwmsg_setze_stichtag_info`(
    IN p_eintrags_nr STRING,
    IN p_stichtag_value STRING, -- e.g., '2023-10-26'
    IN p_stichtag_format STRING, -- e.g., '%Y-%m-%d'
    IN p_info_text STRING
)
BEGIN
    DECLARE current_zusatz_infos STRING;
    DECLARE new_zusatz_infos STRING;
    DECLARE formatted_stichtag STRING;

    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_eintrags_nr cannot be empty or NULL.';
    END IF;

    -- Format the stichtag if provided
    IF p_stichtag_value IS NOT NULL AND p_stichtag_format IS NOT NULL THEN
        SET formatted_stichtag = FORMAT_TIMESTAMP(p_stichtag_format, PARSE_TIMESTAMP(p_stichtag_format, p_stichtag_value));
    ELSE
        SET formatted_stichtag = p_stichtag_value;
    END IF;

    SELECT zusatz_infos INTO current_zusatz_infos
    FROM `my_project.my_dataset.bert_meldung`
    WHERE eintrags_nr = p_eintrags_nr;

    -- Initialize or append to JSON string
    IF current_zusatz_infos IS NULL OR TRIM(current_zusatz_infos) = '' THEN
        SET new_zusatz_infos = JSON_OBJECT('stichtag', formatted_stichtag, 'stichtag_info_text', p_info_text);
    ELSE
        -- Attempt to parse as JSON and update, or concatenate if not valid JSON
        BEGIN
            -- Try to parse as JSON
            DECLARE temp_json JSON;
            SET temp_json = PARSE_JSON(current_zusatz_infos);
            SET new_zusatz_infos = TO_JSON_STRING(JSON_SET(temp_json, '$."stichtag"', formatted_stichtag, '$."stichtag_info_text"', p_info_text));
        EXCEPTION WHEN ERROR THEN
            -- If not valid JSON, just append as a string
            SET new_zusatz_infos = CONCAT(current_zusatz_infos, ' | Stichtag: ', formatted_stichtag, ' Info: ', p_info_text);
        END;
    END IF;

    UPDATE `my_project.my_dataset.bert_meldung`
    SET
        zusatz_infos = new_zusatz_infos,
        last_update_timestamp = CURRENT_TIMESTAMP()
    WHERE
        eintrags_nr = p_eintrags_nr;
END;