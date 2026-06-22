-- Migrated from vobs/dw_source/isrpt/isbert/install_save/d_ausd_bp_ta_tarifoption.sql
-- Job: DW.BERT_AUSD_BP_TA_TARIFOPTION

-- Declare v_datum parameter. This will be supplied by the Airflow DAG.
DECLARE v_datum STRING DEFAULT @v_datum_param;

-- Step 1: Drop existing target tables to ensure idempotency
DROP TABLE IF EXISTS `bert_staging.bpr_opt_filter`;
DROP TABLE IF EXISTS `bert_reporting.tarifoption`;

-- Step 2: Create the intermediate filter table `bert_staging.bpr_opt_filter`
-- This table combines tariff option data with category information.
-- The ORDER BY clause is crucial here to mimic the original Oracle behavior
-- for the subsequent aggregation, even though BigQuery's STRING_AGG
-- directly handles grouping.
CREATE OR REPLACE TABLE `bert_staging.bpr_opt_filter` AS
SELECT
    t.bpr_id,
    t.cntrct_id,
    t.pds_description,
    l.opt_kategorie
FROM
    `bert_master.sof_l_bpr_optionen_filter` AS l
INNER JOIN
    `bert_raw.sof_ta_bpr_opt_` || v_datum AS t -- Dynamic table name using v_datum
    ON t.bpr_id = l.bpr_id
ORDER BY
    t.cntrct_id,
    t.pds_description
;

-- Step 3: Create the final tariff option table `bert_reporting.tarifoption`
-- This aggregates the descriptions into comma-separated strings based on categories
-- and contract IDs. The STRING_AGG function replaces the complex LEAD function
-- and custom Oracle concatenation functions.
CREATE OR REPLACE TABLE `bert_reporting.tarifoption` AS
SELECT
    cntrct_id,
    SUBSTR(TRIM(STRING_AGG(CASE WHEN opt_kategorie = 'BUDGET' THEN pds_description ELSE NULL END, ', ' ORDER BY pds_description IGNORE NULLS)), 1, 500) AS business_option,
    SUBSTR(TRIM(STRING_AGG(CASE WHEN opt_kategorie = 'SONST' THEN pds_description ELSE NULL END, ', ' ORDER BY pds_description IGNORE NULLS)), 1, 500) AS sonstige_option,
    SUBSTR(TRIM(STRING_AGG(CASE WHEN opt_kategorie = 'GPRS' THEN pds_description ELSE NULL END, ', ' ORDER BY pds_description IGNORE NULLS)), 1, 500) AS gprs_option
FROM
    `bert_staging.bpr_opt_filter`
GROUP BY
    cntrct_id
;