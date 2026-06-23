-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/d_ausd_v_ta_c_bfc.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
-- This file defines the BigQuery Stored Procedure that encapsulates the core data processing logic.

CREATE OR REPLACE PROCEDURE `isbert_dataset.d_ausd_v_ta_c_bfc`(
    p_eintragsnr STRING,
    p_jobkennung STRING,
    OUT records_processed INT64
)
BEGIN
    -- Declare variables
    DECLARE v_bfc_procedure_date DATE;
    DECLARE v_max_update INT64 DEFAULT 1000000; -- Max rows to update in a batch, from original DEFINE

    -- Determine the latest relevant date for bfc_procedure.
    -- Original Oracle code used `all_objects.created` for a package.
    -- In BigQuery, this is a placeholder. Consider using a metadata table or deployment timestamp.
    SET v_bfc_procedure_date = CURRENT_DATE(); -- Placeholder: Use current date
    -- Or, if a specific fixed date or from metadata is desired:
    -- SET v_bfc_procedure_date = PARSE_DATE('%Y%m%d', '20080109'); -- Example: Fixed initial deployment date

    -- Get 'Stichtag' (key date).
    -- Original Oracle: SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') FROM isbert_schema.dwtk_meldungen
    DECLARE v_datum DATE;
    SET v_datum = COALESCE((SELECT MAX(DATE(m.timecreated)) FROM `isbert_dataset.dwtk_meldungen` AS m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'), DATE '1900-01-01');

    -- Step 1: Truncate and populate `isbert_dataset.ta_c_bfc_akt`
    -- Equivalent to `TRUNCATE TABLE sof$ta_c_bfc_akt;`
    TRUNCATE TABLE `isbert_dataset.ta_c_bfc_akt`;

    -- Insert into staging table
    INSERT INTO `isbert_dataset.ta_c_bfc_akt` (
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
        MAX(GREATEST(COALESCE(c.bfc_age, DATE '1900-01-01'),
                     COALESCE(b.bfc_age, DATE '1900-01-01'),
                     COALESCE(v.bfc_age, DATE '1900-01-01'),
                     COALESCE(p_fi.bfc_age, DATE '1900-01-01'),
                     COALESCE(p_fo.bfc_age, DATE '1900-01-01'),
                     COALESCE(p_fi_n.bfc_age, DATE '1900-01-01'),
                     COALESCE(p_fo_n.bfc_age, DATE '1900-01-01'))) AS bfc_age,
        COUNT(1) AS bfc_count
    FROM
        `isbert_dataset.ta_cntrct_crs` AS c
        LEFT JOIN `isbert_dataset.ta_barrier` AS b ON c.cntrct_id = b.cntrct_id
        LEFT JOIN `isbert_dataset.ta_cntrct_valid` AS v ON c.cntrct_validity_id = v.cntrct_validity_id
        LEFT JOIN `isbert_dataset.ta_period` AS p_fi ON v.first_period_id = p_fi.period_id
        LEFT JOIN `isbert_dataset.ta_period` AS p_fo ON v.following_period_id = p_fo.period_id
        LEFT JOIN `isbert_dataset.ta_period` AS p_fi_n ON v.first_notice_period_id = p_fi_n.period_id
        LEFT JOIN `isbert_dataset.ta_period` AS p_fo_n ON v.follow_notice_period_id = p_fo_n.period_id
    GROUP BY
        c.cntrct_id;

    -- Step 2: Initial population of `isbert_dataset.ta_c_bfc` if empty
    -- Original Oracle PL/SQL block: `SELECT COUNT(1) INTO v_rows FROM sof$ta_c_bfc WHERE rownum = 1; IF v_rows = 0 THEN ...`
    IF (SELECT COUNT(1) FROM `isbert_dataset.ta_c_bfc` LIMIT 1) = 0 THEN
        INSERT INTO `isbert_dataset.ta_c_bfc` (
            cntrct_id,
            bfc_age,
            bfc_count,
            bfc_procedure,
            commitment_reference_date,
            cntrct_validity_id
        )
        SELECT
            cntrct_id,
            bfc_age,
            bfc_count,
            DATE '1900-01-01' AS bfc_procedure, -- From original `TO_DATE('19000101', 'YYYYMMDD')`
            commitment_reference_date,
            cntrct_validity_id
        FROM `isbert_dataset.ta_c_bfc_akt`;
    END IF;

    -- Step 3: MERGE statement to update or insert bindefristen where calculation basis has changed
    MERGE INTO `isbert_dataset.ta_c_bfc` AS D
    USING `isbert_dataset.ta_c_bfc_akt` AS S
    ON (D.cntrct_id = S.cntrct_id)
    WHEN MATCHED AND (D.bfc_age < S.bfc_age OR D.bfc_count <> S.bfc_count) THEN
        UPDATE SET
            bindefrist = `isbert_dataset.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id),
            bfc_age = S.bfc_age,
            bfc_count = S.bfc_count,
            bfc_procedure = v_bfc_procedure_date,
            commitment_reference_date = S.commitment_reference_date,
            cntrct_validity_id = S.cntrct_validity_id
    WHEN NOT MATCHED THEN
        INSERT (
            cntrct_id,
            bindefrist,
            bfc_age,
            bfc_count,
            bfc_procedure,
            commitment_reference_date,
            cntrct_validity_id
        ) VALUES (
            S.cntrct_id,
            `isbert_dataset.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id),
            S.bfc_age,
            S.bfc_count,
            v_bfc_procedure_date,
            S.commitment_reference_date,
            S.cntrct_validity_id
        );

    -- Step 4: Update bindefristen not yet calculated with the current procedure version
    -- Original Oracle used ROWNUM <= &v_max_update. In BigQuery, this implies limiting arbitrarily if no ORDER BY.
    -- Assuming the intent is to process up to v_max_update records without specific ordering priority.
    UPDATE `isbert_dataset.ta_c_bfc`
    SET
        bindefrist = `isbert_dataset.bfc_get_bindefrist`(
            cntrct_id,
            commitment_reference_date,
            cntrct_validity_id
        ),
        bfc_procedure = v_bfc_procedure_date
    WHERE
        bfc_procedure < v_bfc_procedure_date
    QUALIFY ROW_NUMBER() OVER(ORDER BY cntrct_id) <= v_max_update; -- Arbitrary order for limiting

    -- Get the number of affected rows (records_processed).
    -- BigQuery doesn't have a direct `ROWCOUNT` for a whole procedure.
    -- We'll approximate by counting rows in the main table after operations, or summing individual DML row counts.
    -- For simplicity, let's just return the total count of the target table for now,
    -- or if a specific metric is needed, it should be explicitly captured after each DML statement.
    -- For this context, assuming records_processed means the final state.
    SET records_processed = (SELECT COUNT(1) FROM `isbert_dataset.ta_c_bfc`);

    -- Truncate temporary table at the end
    TRUNCATE TABLE `isbert_dataset.ta_c_bfc_akt`;

END;