-- BigQuery SQL equivalent of d_ausd_v_ta_cntrct_templ.sql
-- This script performs the core data transformation and insertion.
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh

-- Declare a variable for the derived date
DECLARE v_datum_string STRING;
DECLARE v_current_date DATE;

-- Determine the processing date (v_datum)
-- Assumes project.dataset.dwtk_meldungen exists and has a timecreated column
SELECT
    COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
FROM
    `your_project_id.your_dataset_id.dwtk_meldungen` AS m
WHERE
    m.job_kennung = 'BERT_DROP_TEMP_TABLE'
INTO
    v_datum_string;

SET v_current_date = PARSE_DATE('%Y%m%d', v_datum_string);

-- Truncate the target table before insertion
TRUNCATE TABLE `your_project_id.your_dataset_id.ta_cntrct_templ`;

-- Insert data into the target table
INSERT INTO `your_project_id.your_dataset_id.ta_cntrct_templ`
(cntrct_template_id, cds_description_id, cds_description)
SELECT
    ct.cntrct_template_id,
    ct.cds_description_id,
    cd.cds_description
FROM
    `your_project_id.your_dataset_id.cds_ta_cntrct_template` AS ct
JOIN
    `your_project_id.your_dataset_id.cds_ta_care_description` AS cd
ON
    ct.cds_description_id = cd.cds_description_id
WHERE
    ct.insert_at <= v_current_date
AND
    (ct.modified_at IS NULL OR ct.modified_at > v_current_date)
AND
    ct.valid_from <= v_current_date
AND
    (ct.valid_to IS NULL OR ct.valid_to > v_current_date)
AND
    ct.is_production = 1
AND
    cd.language = 1;