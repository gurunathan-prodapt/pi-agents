-- BigQuery SQL for Step 3: Merge Changed Rows into ta_c_bfc
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- This script merges data from the staging table into the target table,
-- updating existing rows and inserting new ones.

MERGE INTO `{{ project_id }}.{{ dataset_id }}.ta_c_bfc` AS D
USING `{{ project_id }}.{{ dataset_id }}.ta_c_bfc_akt` AS S
ON (
    D.cntrct_id = S.cntrct_id
)
WHEN MATCHED AND (
       D.bfc_age < S.bfc_age
    OR D.bfc_count <> S.bfc_count
) THEN
    UPDATE SET
        bindefrist = `{{ project_id }}.{{ dataset_id }}.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id),
        bfc_age = S.bfc_age,
        bfc_count = S.bfc_count,
        bfc_procedure = CURRENT_DATE(), -- Replaces TO_DATE(&v_bfc_procedure, 'YYYYMMDD')
        commitment_reference_date = S.commitment_reference_date,
        cntrct_validity_id = S.cntrct_validity_id,
        load_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        cntrct_id,
        bindefrist,
        bfc_age,
        bfc_count,
        bfc_procedure,
        commitment_reference_date,
        cntrct_validity_id,
        load_ts
    )
    VALUES (
        S.cntrct_id,
        `{{ project_id }}.{{ dataset_id }}.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id),
        S.bfc_age,
        S.bfc_count,
        CURRENT_DATE(), -- Replaces TO_DATE(&v_bfc_procedure, 'YYYYMMDD')
        S.commitment_reference_date,
        S.cntrct_validity_id,
        CURRENT_TIMESTAMP()
    );