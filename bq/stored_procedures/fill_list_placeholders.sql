-- BigQuery Stored Procedure: fill_list_placeholders
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh (parser_fillist function)
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Description: Replaces list placeholders (<FIRST>, <MIDDLE>, <END>, <SINGLE>) within a template.
--              Assumes `list_name` refers to a list of elements in the `list_data` table.

CREATE OR REPLACE PROCEDURE `<PROJECT_ID>.<DATASET_ID>.fill_list_placeholders`(
    INOUT io_template STRING,
    IN p_list_name STRING
)
BEGIN
    DECLARE v_list_elements ARRAY<STRING>;
    DECLARE v_list_size INT64;

    -- Fetch list elements
    SET v_list_elements = ARRAY(SELECT element_value FROM `<PROJECT_ID>.<DATASET_ID>.list_data` WHERE list_name = p_list_name ORDER BY element_order);
    SET v_list_size = ARRAY_LENGTH(v_list_elements);

    IF v_list_size = 1 THEN
        SET io_template = REPLACE(io_template, '<SINGLE>', v_list_elements[OFFSET(0)]);
        SET io_template = REPLACE(io_template, '<FIRST>', '');
        SET io_template = REPLACE(io_template, '<MIDDLE>', '');
        SET io_template = REPLACE(io_template, '<END>', '');
    ELSEIF v_list_size > 1 THEN
        -- Handle <FIRST>
        SET io_template = REPLACE(io_template, '<FIRST>', v_list_elements[OFFSET(0)]);

        -- Handle <MIDDLE>
        -- Use a temporary placeholder to avoid issues if middle elements also contain '<MIDDLE>'
        DECLARE temp_middle_elements STRING;
        SET temp_middle_elements = '';
        FOR i IN 1 TO v_list_size - 2 DO
            SET temp_middle_elements = CONCAT(temp_middle_elements, v_list_elements[OFFSET(i)], '\n'); -- Assuming newline separation for multiple MIDDLE elements
        END FOR;
        SET io_template = REPLACE(io_template, '<MIDDLE>', TRIM(TRAILING '\n' FROM temp_middle_elements));

        -- Handle <END>
        SET io_template = REPLACE(io_template, '<END>', v_list_elements[OFFSET(v_list_size - 1)]);

        -- Clear <SINGLE>
        SET io_template = REPLACE(io_template, '<SINGLE>', '');
    ELSE
        -- If list is empty, clear all placeholders
        SET io_template = REPLACE(io_template, '<SINGLE>', '');
        SET io_template = REPLACE(io_template, '<FIRST>', '');
        SET io_template = REPLACE(io_template, '<MIDDLE>', '');
        SET io_template = REPLACE(io_template, '<END>', '');
    END IF;
END;