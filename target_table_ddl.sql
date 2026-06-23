-- Legacy Source: vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
-- Job: vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
-- Description: DDL for the BigQuery target data tables.

-- Target table for the processed tariff options
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.sof_ta_tarifoption` (
    cntrct_id INT64 OPTIONS(description="Contract ID"),
    business_option STRING OPTIONS(description="Business options concatenated string"),
    sonstige_option STRING OPTIONS(description="Other options concatenated string"),
    gprs_option STRING OPTIONS(description="GPRS options concatenated string")
);

-- Intermediate table `sof_ta_bpr_opt_filter` (can also be a CTE within the stored procedure)
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.sof_ta_bpr_opt_filter` (
    bpr_id INT64 OPTIONS(description="Basisprodukt ID"),
    cntrct_id INT64 OPTIONS(description="Contract ID"),
    pds_description STRING OPTIONS(description="PDS description"),
    opt_kategorie STRING OPTIONS(description="Option category")
);

-- Assumed source table `dwtk_meldungen` for date derivation
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dwtk_meldungen` (
    job_kennung STRING NOT NULL,
    timecreated TIMESTAMP,
    -- Add other columns as per source schema if necessary
);

-- Assumed source lookup table `sof_ta_l_bpr_optionen_filter`
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.sof_ta_l_bpr_optionen_filter` (
    bpr_id INT64 NOT NULL,
    opt_kategorie STRING NOT NULL,
    -- Add other columns as per source schema if necessary
);

-- Assumed source table `sof_ta_bpr_opt_text` (unified view of sof$ta_bpr_opt_text_&v_datum)
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.sof_ta_bpr_opt_text` (
    bpr_id INT64 NOT NULL,
    cntrct_id INT64 NOT NULL,
    pds_description STRING,
    -- Add other columns as per source schema if necessary
    -- If the original table was truly dynamic by date, consider partitioning this table by date
    -- or creating a view that filters a base table for the current date.
);