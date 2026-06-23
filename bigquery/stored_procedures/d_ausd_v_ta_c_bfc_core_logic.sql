-- BigQuery Stored Procedure encapsulating the core SQL logic from d_ausd_v_ta_c_bfc.sql.
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_c_bfc.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_v_ta_c_bfc_core_logic`(
    IN p_run_id STRING,
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING,
    OUT p_records_processed INT64
)
BEGIN
    -- Variables from original SQL
    DECLARE v_max_update INT64 DEFAULT 1000000;
    DECLARE v_bfc_procedure DATE; -- This needs to be determined dynamically, e.g., creation date of this SP

    -- Determine the current date for v_bfc_procedure (assuming current deployment date of the logic)
    SET v_bfc_procedure = CURRENT_DATE(); -- Or a specific deployment date

    -- Create temporary table sof$ta_c_bfc_akt
    -- This mimics the TRUNCATE TABLE then INSERT pattern
    -- The schema needs to be accurately derived from sof$ta_c_bfc_akt in Oracle
    -- Assuming a target table `project.dataset.sof_ta_c_bfc_akt` for this migration step
    EXECUTE IMMEDIATE """
        TRUNCATE TABLE `project.dataset.sof_ta_c_bfc_akt`;
    """;

    -- Step 1: Populate sof$ta_c_bfc_akt with updated contract data
    -- Original: INSERT /*+ append */ INTO sof$ta_c_bfc_akt
    EXECUTE IMMEDIATE """
        INSERT INTO `project.dataset.sof_ta_c_bfc_akt` (
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
            MAX(GREATEST(COALESCE(c.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                         COALESCE(b.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                         COALESCE(v.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                         COALESCE(p_fi.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                         COALESCE(p_fo.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                         COALESCE(p_fi_n.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                         COALESCE(p_fo_n.bfc_age, PARSE_DATE('%Y%m%d', '19000101')))) AS bfc_age,
            COUNT(1) AS bfc_count
        FROM
            `project.dataset.sof_ta_cntrct_crs` AS c
            LEFT JOIN `project.dataset.sof_ta_barrier` AS b ON c.cntrct_id = b.cntrct_id
            LEFT JOIN `project.dataset.sof_ta_cntrct_valid` AS v ON c.cntrct_validity_id = v.cntrct_validity_id
            LEFT JOIN `project.dataset.sof_ta_period` AS p_fi ON v.first_period_id = p_fi.period_id
            LEFT JOIN `project.dataset.sof_ta_period` AS p_fo ON v.following_period_id = p_fo.period_id
            LEFT JOIN `project.dataset.sof_ta_period` AS p_fi_n ON v.first_notice_period_id = p_fi_n.period_id
            LEFT JOIN `project.dataset.sof_ta_period` AS p_fo_n ON v.follow_notice_period_id = p_fo_n.period_id
        GROUP BY
            c.cntrct_id;
    """;

    -- Step 2: Initial population of sof$ta_c_bfc if empty
    -- Original: DECLARE ... IF v_rows = 0 THEN INSERT ... END IF;
    -- This logic assumes `project.dataset.sof_ta_c_bfc` is the target table.
    -- NOTE: BigQuery MERGE statement can handle initial population better.
    -- For now, mimicking the IF EXISTS logic.
    DECLARE v_sof_ta_c_bfc_count INT64;
    EXECUTE IMMEDIATE """
        SELECT COUNT(1) FROM `project.dataset.sof_ta_c_bfc` LIMIT 1
    """ INTO v_sof_ta_c_bfc_count;

    IF v_sof_ta_c_bfc_count = 0 THEN
        EXECUTE IMMEDIATE """
            INSERT INTO `project.dataset.sof_ta_c_bfc` (
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
                PARSE_DATE('%Y%m%d', '19000101'), -- Placeholder for initial procedure date
                commitment_reference_date,
                cntrct_validity_id
            FROM `project.dataset.sof_ta_c_bfc_akt`;
        """;
    END IF;

    -- Step 3: Merge into sof$ta_c_bfc based on sof$ta_c_bfc_akt
    -- Original: MERGE INTO sof$ta_c_bfc D USING sof$ta_c_bfc_akt S ON (...)
    EXECUTE IMMEDIATE FORMAT("""
        MERGE INTO `project.dataset.sof_ta_c_bfc` AS D
        USING `project.dataset.sof_ta_c_bfc_akt` AS S
        ON (
            D.cntrct_id = S.cntrct_id
        )
        WHEN MATCHED AND (
               D.bfc_age < S.bfc_age
            OR D.bfc_count <> S.bfc_count
        ) THEN UPDATE SET
            bindefrist = `project.dataset.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id),
            bfc_age = S.bfc_age,
            bfc_count = S.bfc_count,
            bfc_procedure = '%s', -- Use FORMAT to inject the date
            commitment_reference_date = S.commitment_reference_date,
            cntrct_validity_id = S.cntrct_validity_id
        WHEN NOT MATCHED THEN INSERT (
            cntrct_id,
            bindefrist,
            bfc_age,
            bfc_count,
            bfc_procedure,
            commitment_reference_date,
            cntrct_validity_id
        ) VALUES (
            S.cntrct_id,
            `project.dataset.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id),
            S.bfc_age,
            S.bfc_count,
            '%s', -- Use FORMAT to inject the date
            S.commitment_reference_date,
            S.cntrct_validity_id
        );
    """, FORMAT_DATE('%Y-%m-%d', v_bfc_procedure), FORMAT_DATE('%Y-%m-%d', v_bfc_procedure));


    -- Step 4: Update bindefristen not yet calculated with the current procedure
    -- Original: UPDATE sof$ta_c_bfc SET ... WHERE bfc_procedure < TO_DATE(&v_bfc_procedure, 'YYYYMMDD') AND ROWNUM <= &v_max_update
    EXECUTE IMMEDIATE FORMAT("""
        UPDATE `project.dataset.sof_ta_c_bfc`
        SET
            bindefrist = `project.dataset.bfc_get_bindefrist`(
                cntrct_id,
                commitment_reference_date,
                cntrct_validity_id
            ),
            bfc_procedure = '%s'
        WHERE
            bfc_procedure < '%s'
        LIMIT %d;
    """, FORMAT_DATE('%Y-%m-%d', v_bfc_procedure), FORMAT_DATE('%Y-%m-%d', v_bfc_procedure), v_max_update);

    -- Step 5: Truncate temporary table sof$ta_c_bfc_akt
    -- Original: TRUNCATE TABLE sof$ta_c_bfc_akt REUSE STORAGE
    EXECUTE IMMEDIATE """
        TRUNCATE TABLE `project.dataset.sof_ta_c_bfc_akt`;
    """;

    -- Capture records processed (approximation, might need more specific counting for each DML)
    -- For now, let's count total rows in the target table as an approximation
    EXECUTE IMMEDIATE """
        SELECT COUNT(1) FROM `project.dataset.sof_ta_c_bfc`
    """ INTO p_records_processed;

END;