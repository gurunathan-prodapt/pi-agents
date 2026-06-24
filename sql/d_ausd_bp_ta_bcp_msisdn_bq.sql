-- Migrated SQL logic from d_ausd_bp_ta_bcp_msisdn.sql to BigQuery Standard SQL
-- Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh

-- This SQL script defines the SELECT statement for the data transformation.
-- The actual INSERT and TRUNCATE operations will be handled by the Airflow DAG.

SELECT DISTINCT
    bp.cntrct_id,
    bp.bpr_id,
    bp.cntrct_id_ref,
    rn.tn_tel_msisdn
FROM
    `YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.SOF_TA_BPR_BCP` bp
JOIN
    `YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.SOF_TA_RN_VERTRAG` rn
ON
    bp.cntrct_id_ref = rn.cntrct_id
;