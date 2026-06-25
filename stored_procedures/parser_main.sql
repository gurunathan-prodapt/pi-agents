-- BigQuery Stored Procedure to orchestrate the template parsing logic.
-- Replicates core functionality of parser, parser_filmeta, and parser_filfile
-- from vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- This procedure handles scalar replacements, list expansions, includes, and timestamp processing.

CREATE OR REPLACE PROCEDURE project.dataset.parser_main(
    template_id_param STRING,
    output_table_name STRING
)
BEGIN
    DECLARE current_text STRING;
    DECLARE previous_text STRING;
    DECLARE iteration INT64 DEFAULT 0;
    DECLARE max_iterations INT64 DEFAULT 10; -- Prevent infinite loops
    DECLARE has_changed BOOL DEFAULT TRUE;

    -- Temporary table to hold current working lines
    CREATE OR REPLACE TEMPORARY TABLE temp_work_lines (
        line_no INT64,
        line_text STRING
    );

    -- Temporary table to hold scalar variables defined dynamically (name=value)
    CREATE OR REPLACE TEMPORARY TABLE temp_scalar_vars (
        var_name STRING,
        var_value STRING,
        PRIMARY KEY (var_name) NOT ENFORCED
    );

    -- Temporary table to hold list definitions dynamically
    CREATE OR REPLACE TEMPORARY TABLE temp_list_defs (
        list_name STRING,
        elements ARRAY<STRING>,
        PRIMARY KEY (list_name) NOT ENFORCED
    );

    -- Initialize temp_work_lines with the base template
    INSERT INTO temp_work_lines (line_no, line_text)
    SELECT line_no, line_text FROM project.dataset.template_lines WHERE template_id = template_id_param;

    -- Populate initial scalar and list values from dedicated tables
    INSERT INTO temp_scalar_vars (var_name, var_value)
    SELECT placeholder_name, placeholder_value FROM project.dataset.scalar_values WHERE template_id = template_id_param;

    INSERT INTO temp_list_defs (list_name, elements)
    SELECT list_name, ARRAY_AGG(element_value ORDER BY element_no)
    FROM project.dataset.list_values WHERE template_id = template_id_param
    GROUP BY list_name;

    -- Main parsing loop
    SET previous_text = ''; -- Initialize to ensure first run
    SET current_text = (SELECT STRING_AGG(line_text ORDER BY line_no, '\n') FROM temp_work_lines);

    WHILE has_changed AND iteration < max_iterations DO
        SET iteration = iteration + 1;
        SET previous_text = current_text;

        -- Step 1: Process dynamic variable definitions (name=value) and update temp_scalar_vars
        -- This needs to happen first to ensure subsequent substitutions use the latest values
        MERGE INTO temp_scalar_vars AS target
        USING (
            SELECT
                REGEXP_EXTRACT(line_text, r'^\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*=\\s*(.*)$') AS var_name,
                REGEXP_EXTRACT(line_text, r'^\\s*[a-zA-Z_][a-zA-Z0-9_]*\\s*=\\s*(.*)$') AS var_value
            FROM temp_work_lines
            WHERE REGEXP_CONTAINS(line_text, r'^\\s*[a-zA-Z_][a-zA-Z0-9_]*\\s*=')
        ) AS source
        ON target.var_name = source.var_name
        WHEN MATCHED THEN
            UPDATE SET var_value = source.var_value
        WHEN NOT MATCHED THEN
            INSERT (var_name, var_value) VALUES (source.var_name, source.var_value);

        -- Step 2: Scalar placeholder replacements
        UPDATE temp_work_lines
        SET line_text = (
            SELECT
                ARRAY_REDUCE(
                    ARRAY_AGG(STRUCT(tsv.var_name, tsv.var_value)),
                    line_text,
                    (accumulator, s) -> REGEXP_REPLACE(accumulator, CONCAT(r'\{\{', s.var_name, r'\}\}'), s.var_value),
                    (accumulator, s) -> accumulator
                )
            FROM temp_scalar_vars AS tsv
            WHERE REGEXP_CONTAINS(line_text, r'\{\{([a-zA-Z_][a-zA-Z0-9_]*)\}\}')
        )
        WHERE EXISTS (
            SELECT 1 FROM temp_scalar_vars AS tsv
            WHERE REGEXP_CONTAINS(temp_work_lines.line_text, CONCAT(r'\{\{', tsv.var_name, r'\}\}'))
        );

        -- Step 3: Handle INCLUDE directives
        -- Replace `INCLUDE <file>` with content from `include_map`
        -- This logic is simplified; original ksh might handle multiple nested includes more intricately.
        UPDATE temp_work_lines
        SET line_text = (
            SELECT im.include_text
            FROM project.dataset.include_map AS im
            WHERE REGEXP_EXTRACT(temp_work_lines.line_text, r'^\\s*INCLUDE\\s+(.+)\\s*$') = im.include_name
        )
        WHERE REGEXP_CONTAINS(line_text, r'^\\s*INCLUDE\\s+(.+)\\s*$')
        AND EXISTS (
            SELECT 1 FROM project.dataset.include_map AS im
            WHERE REGEXP_EXTRACT(temp_work_lines.line_text, r'^\\s*INCLUDE\\s+(.+)\\s*$') = im.include_name
        );


        -- Step 4: Handle TIMESTAMP directives
        -- Replace `TIMESTAMP <expression>` with formatted timestamp
        -- Using CURRENT_TIMESTAMP() for sysdate_str for now, as script's sysdate source is dynamic.
        UPDATE temp_work_lines
        SET line_text = (
            SELECT
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', project.dataset.parse_timestamp_expression(
                    REGEXP_EXTRACT(temp_work_lines.line_text, r'^\\s*TIMESTAMP\\s+(.+)\\s*$'),
                    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP())
                ))
        )
        WHERE REGEXP_CONTAINS(line_text, r'^\\s*TIMESTAMP\\s+(.+)\\s*$');


        -- Step 5: Process LISTDEF directives and update temp_list_defs
        -- This is a more complex multi-line parse, for simplicity, I'll assume LISTDEF defines a list on a single line
        -- The original KSH script parses blocks. A full translation would require more sophisticated block parsing.
        -- Here, we'll try to extract list name and values from a pattern like: LISTDEF MYLIST="val1","val2"
        MERGE INTO temp_list_defs AS target
        USING (
            SELECT
                REGEXP_EXTRACT(line_text, r'^\\s*LISTDEF\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\s*=') AS list_name,
                ARRAY(
                    SELECT TRIM(val, '"') FROM UNNEST(REGEXP_EXTRACT_ALL(line_text, r'"([^"]*)"')) AS val
                ) AS elements
            FROM temp_work_lines
            WHERE REGEXP_CONTAINS(line_text, r'^\\s*LISTDEF\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\s*=\\s*(".*")')
        ) AS source
        ON target.list_name = source.list_name
        WHEN MATCHED THEN
            UPDATE SET elements = source.elements
        WHEN NOT MATCHED THEN
            INSERT (list_name, elements) VALUES (source.list_name, source.elements);

        -- Step 6: List expansion using the UDF
        UPDATE temp_work_lines
        SET line_text = (
            SELECT project.dataset.expand_list_block(temp_work_lines.line_text, tld.list_name, tld.elements)
            FROM temp_list_defs AS tld
            WHERE REGEXP_CONTAINS(temp_work_lines.line_text, CONCAT(r'\{\{', tld.list_name, r'\\[[0-9]+\\]\}\}'))
        )
        WHERE EXISTS (
            SELECT 1 FROM temp_list_defs AS tld
            WHERE REGEXP_CONTAINS(temp_work_lines.line_text, CONCAT(r'\{\{', tld.list_name, r'\\[[0-9]+\\]\}\}'))
        );

        -- Step 7: Handle NOPRINT and PARSEIGNORE directives
        -- NOPRINT: Mark lines for removal
        -- PARSEIGNORE: Mark blocks to be ignored for further processing (not fully supported in this iterative line-by-line model)
        -- For simplicity, NOPRINT lines are removed after each iteration. PARSEIGNORE is ignored in this translation.
        DELETE FROM temp_work_lines
        WHERE REGEXP_CONTAINS(line_text, r'^\\s*NOPRINT\\s*$');


        -- Update current_text for convergence check
        SET current_text = (SELECT STRING_AGG(line_text ORDER BY line_no, '\n') FROM temp_work_lines);

        SET has_changed = (current_text != previous_text);

        -- Ensure loop termination even if convergence isn't perfect, to avoid infinite loops
        IF iteration >= max_iterations THEN
            RAISE NOTICE 'Max iterations reached for template_id: %s', template_id_param;
        END IF;

    END WHILE;

    -- Create or replace the output table with the final processed text
    EXECUTE IMMEDIATE FORMAT("""
        CREATE OR REPLACE TABLE %s AS
        SELECT
            line_no,
            line_text
        FROM temp_work_lines
        ORDER BY line_no;
    """, output_table_name);

    -- Clean up temporary tables
    DROP TABLE temp_work_lines;
    DROP TABLE temp_scalar_vars;
    DROP TABLE temp_list_defs;

END;