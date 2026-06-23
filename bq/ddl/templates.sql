-- DDL for templates table
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- This table stores template content previously read from files by h_alis_parser.ksh.

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.<DATASET_ID>.templates` (
    template_name STRING OPTIONS(description="Unique name of the template"),
    template_content STRING OPTIONS(description="The actual template text with placeholders"),
    description STRING OPTIONS(description="Description of the template's purpose"),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);