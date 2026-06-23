-- BigQuery Stored Procedure: get_node_content
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh (parser_getnode function)
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Description: Extracts content within specified <NODE>...</NODE> tags, supporting <NODE INCLUDE file> directives.
--              This simplified version assumes included files are also available as templates.

CREATE OR REPLACE PROCEDURE `<PROJECT_ID>.<DATASET_ID>.get_node_content`(
    IN p_source_template STRING,
    IN p_node_name STRING,
    OUT o_node_content STRING
)
BEGIN
    DECLARE v_pattern STRING;
    DECLARE v_extracted_block STRING;
    DECLARE v_include_file_name STRING;

    -- Pattern to find the specific node, including potential INCLUDE directive
    SET v_pattern = FORMAT(r"(?sU)\<NODE %s(?:\s+INCLUDE\s+([^\>]+))?\>(.*)\<\/NODE\>", p_node_name);
    SET v_extracted_block = REGEXP_EXTRACT(p_source_template, v_pattern);

    IF v_extracted_block IS NOT NULL THEN
        -- Check for INCLUDE directive
        SET v_include_file_name = REGEXP_EXTRACT(v_extracted_block, r"INCLUDE\s+([^\s\>]+)");
        
        IF v_include_file_name IS NOT NULL THEN
            -- If an include is found, fetch content from the included template
            SELECT template_content
            INTO o_node_content
            FROM `<PROJECT_ID>.<DATASET_ID>.templates`
            WHERE template_name = v_include_file_name;
            
            IF o_node_content IS NULL THEN
                 SET o_node_content = FORMAT("ERROR: Included template '%s' not found.", v_include_file_name);
            END IF;
        ELSE
            -- No include, extract the content directly
            SET o_node_content = REGEXP_EXTRACT(v_source_template, FORMAT(r"(?sU)\<NODE %s\>(.*)\<\/NODE\>", p_node_name));
        END IF;
    ELSE
        SET o_node_content = NULL; -- Node not found
    END IF;
END;