-- BigQuery SQL for Step 2: Initial Population of ta_c_bfc
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- This script conditionally inserts data into the target table if it's empty.

INSERT INTO `{{ project_id }}.{{ dataset_id }}.ta_c_bfc` (
    cntrct_id,
    bfc_age,
    bfc_count,
    bfc_procedure,
    commitment_reference_date,
    cntrct_validity_id
)
SELECT
    akt.cntrct_id,
    akt.bfc_age,
    akt.bfc_count,
    PARSE_DATE('%Y%m%d', '19000101') AS bfc_procedure, -- Corresponds to TO_DATE('19000101', 'YYYYMMDD')
    akt.commitment_reference_date,
    akt.cntrct_validity_id
FROM
    `{{ project_id }}.{{ dataset_id }}.ta_c_bfc_akt` AS akt
WHERE
    (SELECT COUNT(1) FROM `{{ project_id }}.{{ dataset_id }}.ta_c_bfc`) = 0;