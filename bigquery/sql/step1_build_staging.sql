-- BigQuery SQL for Step 1: Build Staging Table ta_c_bfc_akt
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- This script truncates the staging table and inserts aggregated data.

TRUNCATE TABLE `{{ project_id }}.{{ dataset_id }}.ta_c_bfc_akt`;

INSERT INTO `{{ project_id }}.{{ dataset_id }}.ta_c_bfc_akt` (
    cntrct_id,
    commitment_reference_date,
    cntrct_validity_id,
    bfc_age,
    bfc_count
)
SELECT
    c.cntrct_id,
    MAX(c.commitment_reference_date) AS commitment_reference_date,
    MAX(c.cntrct_validity_id) AS cntrct_validity_id,
    MAX(
        GREATEST(
            IFNULL(c.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
            IFNULL(b.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
            IFNULL(v.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
            IFNULL(p_fi.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
            IFNULL(p_fo.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
            IFNULL(p_fi_n.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
            IFNULL(p_fo_n.bfc_age, PARSE_DATE('%Y%m%d', '19000101'))
        )
    ) AS bfc_age,
    COUNT(1) AS bfc_count
FROM
    `{{ project_id }}.{{ dataset_id }}.sof$ta_cntrct_crs` AS c
LEFT JOIN
    `{{ project_id }}.{{ dataset_id }}.sof$ta_barrier` AS b
    ON c.cntrct_id = b.cntrct_id
LEFT JOIN
    `{{ project_id }}.{{ dataset_id }}.sof$ta_cntrct_valid` AS v
    ON c.cntrct_validity_id = v.cntrct_validity_id
LEFT JOIN
    `{{ project_id }}.{{ dataset_id }}.sof$ta_period` AS p_fi
    ON v.first_period_id = p_fi.period_id
LEFT JOIN
    `{{ project_id }}.{{ dataset_id }}.sof$ta_period` AS p_fo
    ON v.following_period_id = p_fo.period_id
LEFT JOIN
    `{{ project_id }}.{{ dataset_id }}.sof$ta_period` AS p_fi_n
    ON v.first_notice_period_id = p_fi_n.period_id
LEFT JOIN
    `{{ project_id }}.{{ dataset_id }}.sof$ta_period` AS p_fo_n
    ON v.follow_notice_period_id = p_fo_n.period_id
GROUP BY
    c.cntrct_id;