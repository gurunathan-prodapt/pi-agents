-- BigQuery Stored Procedure: d_ausd_bp_ta_bpr_instance
-- Legacy Source: d_ausd_bp_ta_bpr_instance.sql (invoked by k_ausd_bp_ta_bpr_instance.ksh)
-- This is a placeholder procedure for the core data manipulation logic.
-- The actual SQL content from the original `d_ausd_bp_ta_bpr_instance.sql` needs to be translated
-- and inserted here.
--
-- Please replace `project.dataset` with your actual GCP Project ID and BigQuery Dataset ID.

CREATE OR REPLACE PROCEDURE `project.dataset`.d_ausd_bp_ta_bpr_instance(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_Stichtag_Raw STRING,  -- Original raw stichtag string (DDMMYYYY)
    p_Stichtag_Date DATE,   -- Parsed date from Stichtag
    p_wiederanlaufWert INT64,
    p_datum_heute DATE,
    p_datum_gestern DATE
)
BEGIN
    -- This procedure should contain the full translation of the original Oracle/legacy SQL script
    -- `d_ausd_bp_ta_bpr_instance.sql` into BigQuery-compatible SQL.
    -- This includes:
    -- 1. Reading from source tables (e.g., `project.dataset.source_table_name`).
    -- 2. Applying transformation logic.
    -- 3. Writing to target tables (e.g., `project.dataset.target_bpr_instance_table`).
    --
    -- Example of data manipulation (REPLACE WITH ACTUAL LOGIC):
    -- INSERT INTO `project.dataset.target_bpr_instance_table` (
    --     job_kennung, entry_nr, reference_date, process_date, ...
    -- )
    -- SELECT
    --     p_JobKennung,
    --     p_EintragsNr,
    --     p_Stichtag_Date,
    --     CURRENT_DATE(),
    --     source_data.col1,
    --     source_data.col2
    -- FROM
    --     `project.dataset.source_table_for_bpr` AS source_data
    -- WHERE
    --     source_data.effective_date = p_Stichtag_Date;

    -- This SELECT statement is a placeholder for debugging/demonstration purposes only.
    -- It should be replaced by your actual data processing logic.
    SELECT FORMAT(
        'Executing d_ausd_bp_ta_bpr_instance for Job: %s, Entry: %s, Stichtag_Raw: %s (Parsed: %s), Wiederanlauf: %d, Heute: %s, Gestern: %s',
        p_JobKennung, p_EintragsNr, p_Stichtag_Raw, p_Stichtag_Date, p_wiederanlaufWert, p_datum_heute, p_datum_gestern
    ) AS debug_message;

    -- If this procedure is meant to return the number of records processed,
    -- you would declare an OUT parameter and set its value here, e.g.:
    -- SET output_records_count = (SELECT COUNT(*) FROM `project.dataset.target_bpr_instance_table` WHERE reference_date = p_Stichtag_Date);

END;