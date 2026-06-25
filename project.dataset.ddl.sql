-- BigQuery DDL for staging tables
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.template_files` (
    file_name STRING NOT NULL,
    line_no INT64 NOT NULL,
    line_text STRING
);

CREATE TABLE IF NOT EXISTS `project.dataset.include_files` (
    file_name STRING NOT NULL,
    line_no INT64 NOT NULL,
    line_text STRING
);

CREATE TABLE IF NOT EXISTS `project.dataset.parser_output` (
    job_id STRING NOT NULL,
    input_file_name STRING NOT NULL,
    output_line_no INT64 NOT NULL,
    output_text STRING
);