-- BigQuery SQL for Step 4: Recalculate Stale Rows in ta_c_bfc
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- This script updates rows where the bfc_procedure is older than the current one,
-- with a limit on the number of updated rows.

UPDATE `{{ project_id }}.{{ dataset_id }}.ta_c_bfc`
SET
    bindefrist = `{{ project_id }}.{{ dataset_id }}.bfc_get_bindefrist`(
        cntrct_id,
        commitment_reference_date,
        cntrct_validity_id
    ),
    bfc_procedure = CURRENT_DATE(), -- Replaces TO_DATE(&v_bfc_procedure, 'YYYYMMDD')
    load_ts = CURRENT_TIMESTAMP()
WHERE
    bfc_procedure < CURRENT_DATE() -- Corresponds to bfc_procedure < TO_DATE(&v_bfc_procedure, 'YYYYMMDD')
QUALIFY
    ROW_NUMBER() OVER(ORDER BY cntrct_id) <= {{ v_max_update }};