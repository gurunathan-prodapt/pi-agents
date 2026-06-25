-- Main Stored Procedure: parser_main
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.parser_main`(
    IN p_job_id STRING,
    IN p_input_file STRING,
    IN p_input_node STRING,
    IN p_output_mode STRING
)
BEGIN
    DECLARE v_current_template_content STRING;
    DECLARE v_processed_lines ARRAY<STRING>;
    DECLARE v_output_lines ARRAY<STRING>;
    DECLARE v_line STRING;
    DECLARE v_line_idx INT64;
    DECLARE v_total_lines INT64;

    DECLARE v_var_map ARRAY<STRUCT<key STRING, value STRING>>;
    DECLARE v_list_map ARRAY<STRUCT<list_name STRING, list_values ARRAY<STRING>>>;

    DECLARE v_temp_output_line STRING;
    DECLARE v_match ARRAY<STRING>;
    DECLARE v_file_content ARRAY<STRING>;

    -- Regular expressions for directives
    DECLARE REGEX_VAR_ASSIGNMENT STRING DEFAULT r'^<(\w+)\s*=\s*"(.*?)"\s*>$';
    DECLARE REGEX_LIST_DEF STRING DEFAULT r'^<LISTDEF\s+(\w+)\s+FILE\s+"(.*?)"\s*>$';
    DECLARE REGEX_INCLUDE STRING DEFAULT r'^<INCLUDE\s+FILE\s+"(.*?)"\s*>$';
    DECLARE REGEX_TIMESTAMP STRING DEFAULT r'^<TIMESTAMP>$';

    -- 1. Read input file content
    SELECT STRING_AGG(line_text, '\n' ORDER BY line_no)
    INTO v_current_template_content
    FROM `project.dataset.template_files`
    WHERE file_name = p_input_file;

    IF v_current_template_content IS NULL THEN
        RAISE BQ.EXCEPTION(ERROR_MESSAGE => CONCAT('Input file not found: ', p_input_file));
    END IF;

    -- 2. If input_node is provided, extract relevant block
    IF p_input_node IS NOT NULL AND p_input_node != '' THEN
        SET v_current_template_content = `project.dataset.extract_node`(v_current_template_content, p_input_node);
        IF v_current_template_content IS NULL THEN
             RAISE BQ.EXCEPTION(ERROR_MESSAGE => CONCAT('Node "', p_input_node, '" not found in file: ', p_input_file));
        END IF;
    END IF;

    SET v_processed_lines = SPLIT(v_current_template_content, '\n');
    SET v_total_lines = ARRAY_LENGTH(v_processed_lines);
    SET v_line_idx = 0;
    SET v_output_lines = [];
    SET v_var_map = [];
    SET v_list_map = [];

    -- Loop through each line of the template
    WHILE v_line_idx < v_total_lines DO
        SET v_line = v_processed_lines[OFFSET(v_line_idx)];
        SET v_temp_output_line = v_line; -- Start with the current line

        -- Handle directives
        -- Variable Assignment: <VAR = "VALUE">
        SET v_match = REGEXP_EXTRACT_ALL(v_line, REGEX_VAR_ASSIGNMENT);
        IF ARRAY_LENGTH(v_match) > 0 THEN
            SET v_var_map = ARRAY_CONCAT(v_var_map, [STRUCT(v_match[OFFSET(0)] AS key, v_match[OFFSET(1)] AS value)]);
            SET v_line_idx = v_line_idx + 1;
            CONTINUE; -- Directive processed, move to next line
        END IF;

        -- List Definition: <LISTDEF LISTNAME FILE "filename">
        SET v_match = REGEXP_EXTRACT_ALL(v_line, REGEX_LIST_DEF);
        IF ARRAY_LENGTH(v_match) > 0 THEN
            DECLARE list_name_to_def STRING = v_match[OFFSET(0)];
            DECLARE list_file_to_read STRING = v_match[OFFSET(1)];
            DECLARE current_list_elements ARRAY<STRING>;

            -- Assuming LISTDEF files are also in template_files or include_files and contain one list item per line
            SELECT ARRAY_AGG(line_text ORDER BY line_no)
            INTO current_list_elements
            FROM `project.dataset.template_files`
            WHERE file_name = list_file_to_read;

            IF current_list_elements IS NULL THEN
                RAISE BQ.EXCEPTION(ERROR_MESSAGE => CONCAT('LISTDEF file not found: ', list_file_to_read));
            END IF;

            SET v_list_map = ARRAY_CONCAT(v_list_map, [STRUCT(list_name_to_def AS list_name, current_list_elements AS list_values)]);
            SET v_line_idx = v_line_idx + 1;
            CONTINUE; -- Directive processed, move to next line
        END IF;

        -- Include Directive: <INCLUDE FILE "filename">
        SET v_match = REGEXP_EXTRACT_ALL(v_line, REGEX_INCLUDE);
        IF ARRAY_LENGTH(v_match) > 0 THEN
            DECLARE include_file_to_read STRING = v_match[OFFSET(0)];
            DECLARE include_content ARRAY<STRING>;

            SELECT ARRAY_AGG(line_text ORDER BY line_no)
            INTO include_content
            FROM `project.dataset.include_files`
            WHERE file_name = include_file_to_read;

            IF include_content IS NULL THEN
                RAISE BQ.EXCEPTION(ERROR_MESSAGE => CONCAT('Include file not found: ', include_file_to_read));
            END IF;

            -- Insert included content into the processing stream right after the current line
            SET v_processed_lines = ARRAY_CONCAT(
                ARRAY_SLICE(v_processed_lines, 0, v_line_idx + 1),
                include_content,
                ARRAY_SLICE(v_processed_lines, v_line_idx + 1)
            );
            SET v_total_lines = ARRAY_LENGTH(v_processed_lines); -- Update total lines after insertion
            -- Do not increment v_line_idx here; the next loop iteration will process the first line of the included content
            -- at the current v_line_idx.
            CONTINUE;
        END IF;

        -- Timestamp Directive: <TIMESTAMP>
        IF REGEXP_CONTAINS(v_line, REGEX_TIMESTAMP) THEN
            SET v_temp_output_line = `project.dataset.replace_placeholder`(v_line, '<TIMESTAMP>', FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()));
        END IF;

        -- Apply scalar variable replacements
        FOR record IN (SELECT key, value FROM UNNEST(v_var_map)) DO
            SET v_temp_output_line = `project.dataset.replace_placeholder`(v_temp_output_line, CONCAT('<', record.key, '>'), record.value);
        END FOR;

        -- Apply list variable replacements (e.g., <FIRST>, <MIDDLE>, <END>, <SINGLE>)
        -- This logic assumes that the template line will use generic list placeholders
        -- and the *first* list in v_list_map that matches a placeholder should be used.
        -- This is a simplification; a more robust solution might require knowing which
        -- list applies to a specific block of template code.
        FOR record IN (SELECT list_name, list_values FROM UNNEST(v_list_map)) DO
            IF REGEXP_CONTAINS(v_temp_output_line, '<(FIRST|MIDDLE|END|SINGLE)>') THEN -- Check if any list placeholder exists
                SET v_temp_output_line = `project.dataset.expand_list_template`(v_temp_output_line, record.list_values);
                BREAK; -- Assume only one list context applies per line for these generic placeholders
            END IF;
        END FOR;

        -- Add processed line to output
        SET v_output_lines = ARRAY_CONCAT(v_output_lines, [v_temp_output_line]);
        SET v_line_idx = v_line_idx + 1;
    END WHILE;

    -- Write output to the parser_output table
    INSERT INTO `project.dataset.parser_output` (job_id, input_file_name, output_line_no, output_text)
    SELECT
        p_job_id AS job_id,
        p_input_file AS input_file_name,
        idx AS output_line_no,
        line AS output_text
    FROM UNNEST(v_output_lines) AS line WITH OFFSET AS idx;

END;