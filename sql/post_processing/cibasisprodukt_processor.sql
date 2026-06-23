-- BigQuery SQL for optional post-processing, replacing commented shell script logic.
-- This script assumes that 'cibasis_data24', 'cibasis_data96', and 'cibasis_fax'
-- are BigQuery tables in 'your_project_id.your_dataset_id' which correspond
-- to the intermediate files used in the original shell script.
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

-- DDL for assumed input tables for post-processing:
-- This is an assumption based on the shell script's file names.
-- You might need to adjust column names and types based on actual data structure.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.cibasis_data24`
(
    id STRING,
    data_field_24_1 STRING,
    data_field_24_2 STRING -- Assuming semicolon-separated fields
);

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.cibasis_data96`
(
    id STRING,
    data_field_96_1 STRING,
    data_field_96_2 STRING -- Assuming semicolon-separated fields
);

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.cibasis_fax`
(
    id STRING,
    fax_data STRING
);

-- Main processing logic:
-- Step 1: "sed s/\\ //g" - Remove spaces from relevant columns.
-- For simplicity, we'll assume the 'id' field is the primary key for joining
-- and that other fields might also need space removal.
-- The original sed was applied to the entire file, so we apply REPLACE to all STRING columns.

-- Intermediate result 1: Cleaned and distinct cibasis_data24
CREATE OR REPLACE TEMPORARY TABLE `cibasis_data24_cleaned` AS
SELECT DISTINCT
    REPLACE(id, ' ', '') AS id,
    REPLACE(data_field_24_1, ' ', '') AS data_field_24_1,
    REPLACE(data_field_24_2, ' ', '') AS data_field_24_2
FROM `your_project_id.your_dataset_id.cibasis_data24`;

-- Intermediate result 2: Cleaned and distinct cibasis_data96
CREATE OR REPLACE TEMPORARY TABLE `cibasis_data96_cleaned` AS
SELECT DISTINCT
    REPLACE(id, ' ', '') AS id,
    REPLACE(data_field_96_1, ' ', '') AS data_field_96_1,
    REPLACE(data_field_96_2, ' ', '') AS data_field_96_2
FROM `your_project_id.your_dataset_id.cibasis_data96`;

-- Intermediate result 3: Cleaned and distinct cibasis_fax
CREATE OR REPLACE TEMPORARY TABLE `cibasis_fax_cleaned` AS
SELECT DISTINCT
    REPLACE(id, ' ', '') AS id,
    REPLACE(fax_data, ' ', '') AS fax_data
FROM `your_project_id.your_dataset_id.cibasis_fax`;


-- Step 2: "join -j1 1 -j2 1 -o 2.1,1.2,2.2 -a 2 -t ';'"
-- This implies joining data24 and data96 first, then that result with fax.
-- The '-o' option specifies output fields: 2.1 (id from 2nd file), 1.2 (field 2 from 1st file), 2.2 (field 2 from 2nd file).
-- The '-a 2' means also print unpairable lines from file 2 (right outer join if file1 is left).
-- Let's interpret this as a series of FULL OUTER JOINs to preserve all data.

-- First join: cibasis_data24 and cibasis_data96
CREATE OR REPLACE TEMPORARY TABLE `cibasis_24_96_tmp` AS
SELECT
    COALESCE(d24.id, d96.id) AS id, -- Key from either table
    d24.data_field_24_1,
    d24.data_field_24_2,
    d96.data_field_96_1,
    d96.data_field_96_2
FROM
    `cibasis_data24_cleaned` d24
FULL OUTER JOIN
    `cibasis_data96_cleaned` d96
ON
    d24.id = d96.id;

-- Second join: cibasis_24_96_tmp with cibasis_fax
-- The original join command was complex:
-- join -j1 1 -j2 1 -o 1.1,1.2,1.3,2.2 -a 1 -t ';' cibasis_24_96.tmp cibasis_fax.dat
-- This means:
-- 1.1: id from cibasis_24_96.tmp
-- 1.2: field 2 from cibasis_24_96.tmp (data_field_24_1)
-- 1.3: field 3 from cibasis_24_96.tmp (data_field_24_2)
-- 2.2: field 2 from cibasis_fax.dat (fax_data)
-- -a 1: Also print unpairable lines from file 1 (LEFT OUTER JOIN if file2 is right)
CREATE OR REPLACE TABLE `your_project_id.your_dataset_id.cibasisprodukt` AS
SELECT
    tmp.id,
    tmp.data_field_24_1,
    tmp.data_field_24_2,
    tmp.data_field_96_1,
    tmp.data_field_96_2,
    fax.fax_data
FROM
    `cibasis_24_96_tmp` tmp
LEFT OUTER JOIN
    `cibasis_fax_cleaned` fax
ON
    tmp.id = fax.id;

-- If CSV export is required (equivalent to original `cibasisprodukt.csv` output):
-- EXPORT DATA OPTIONS(
--   uri='gs://your_gcs_bucket/path/cibasisprodukt.csv',
--   format='CSV',
--   overwrite=TRUE,
--   header=TRUE
-- ) AS
-- SELECT * FROM `your_project_id.your_dataset_id.cibasisprodukt`;