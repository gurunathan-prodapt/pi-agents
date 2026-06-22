-- BigQuery SQL transformation for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_templ.sql
-- This script performs the core data transformation.

-- The processing date `v_datum` and `gcp_project_id` will be passed as parameters from Airflow.

-- Truncate and load into the target table
-- Replaces 'TRUNCATE TABLE sof$ta_cntrct_templ' and subsequent INSERT INTO
CREATE OR REPLACE TABLE `{{ params.gcp_project_id }}.curated.final_fact_table` AS
SELECT
    ct.cntrct_template_id,
    ct.cds_description_id,
    cd.cds_description
FROM
    `{{ params.gcp_project_id }}.staging.cds_ta_cntrct_template_stg` AS ct
JOIN
    `{{ params.gcp_project_id }}.staging.cds_ta_care_description_stg` AS cd
ON
    ct.cds_description_id = cd.cds_description_id
WHERE
    ct.insert_at <= PARSE_DATE('%Y%m%d', '{{ params.v_datum }}')
AND
    (   ct.modified_at IS NULL
     OR ct.modified_at > PARSE_DATE('%Y%m%d', '{{ params.v_datum }}') )
AND
    ct.valid_from <= PARSE_DATE('%Y%m%d', '{{ params.v_datum }}')
AND
    (   ct.valid_to IS NULL
     OR ct.valid_to > PARSE_DATE('%Y%m%d', '{{ params.v_datum }}') )
AND
    ct.is_production = 1
AND
    cd.language = 1;