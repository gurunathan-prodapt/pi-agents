-- BigQuery DDL for staging tables
-- Migrates from vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

CREATE SCHEMA IF NOT EXISTS project.dataset;

-- Table to store template lines, analogous to the script's STDIN input
CREATE OR REPLACE TABLE project.dataset.template_lines (
    template_id STRING NOT NULL,
    line_no INT64 NOT NULL,
    line_text STRING,
    PRIMARY KEY (template_id, line_no) NOT ENFORCED
);

-- Table to store content of files intended for inclusion
CREATE OR REPLACE TABLE project.dataset.include_map (
    include_name STRING NOT NULL, -- Corresponds to file paths in original script
    include_text STRING,
    PRIMARY KEY (include_name) NOT ENFORCED
);

-- Table to store specific meta-block content
CREATE OR REPLACE TABLE project.dataset.meta_blocks (
    template_id STRING NOT NULL,
    block_name STRING NOT NULL,
    line_no INT64 NOT NULL,
    block_text STRING,
    PRIMARY KEY (template_id, block_name, line_no) NOT ENFORCED
);

-- Table to store elements for list expansions
CREATE OR REPLACE TABLE project.dataset.list_values (
    template_id STRING NOT NULL,
    list_name STRING NOT NULL,
    element_no INT64 NOT NULL,
    element_value STRING,
    PRIMARY KEY (template_id, list_name, element_no) NOT ENFORCED
);

-- Table to store key-value pairs for scalar attribute substitutions
CREATE OR REPLACE TABLE project.dataset.scalar_values (
    template_id STRING NOT NULL,
    placeholder_name STRING NOT NULL,
    placeholder_value STRING,
    PRIMARY KEY (template_id, placeholder_name) NOT ENFORCED
);