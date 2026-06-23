-- BigQuery Stored Procedure for d_ausd_bp_ta_bpr_instance_core
-- Translates the core SQL logic from d_ausd_bp_ta_bpr_instance.sql to BigQuery SQL.
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/d_ausd_bp_ta_bpr_instance.sql
-- Orchestrator: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh

CREATE OR REPLACE PROCEDURE my_gcp_project.my_bq_dataset.d_ausd_bp_ta_bpr_instance_core(
    IN stichtag DATE,
    OUT processed_records INT64
)
BEGIN
    -- Step01: Truncate the staging table
    TRUNCATE TABLE my_gcp_project.my_bq_dataset.sof_ta_bpr_instance_staging;

    -- Step03: Insert data into the staging table
    INSERT INTO my_gcp_project.my_bq_dataset.sof_ta_bpr_instance_staging
    (
        CNTRCT_ID,
        BPR_ID,
        BPR_INSTANCE_ID,
        ICCID,
        IMSI_MCC,
        IMSI_MNC,
        IMSI_HLR,
        IMSI_SI,
        CNTRCT_ID_REF,
        processing_date
    )
    SELECT
        bp.cntrct_id,
        bp.bpr_id,
        bp.bpri_com_id AS bpr_instance_id,
        CONCAT(bp.iccid_mi, '-', bp.iccid_ii, '-', bp.iccid_iai, '-', bp.iccid_nr, '-', bp.iccid_cd) AS iccid,
        bp.imsi_mcc,
        bp.imsi_mnc,
        bp.imsi_hlr,
        bp.imsi_si,
        bp.cntrct_id_ref,
        stichtag -- Using the input stichtag for the processing_date
    FROM
        my_gcp_project.my_bq_dataset.cds_ta_cntrct AS c
    JOIN
        my_gcp_project.my_bq_dataset.pds_ta_bpri_com AS bp
    ON
        c.cntrct_id = bp.cntrct_id
    WHERE
        c.cntrct_st IN (5, 6) -- nur Vertragsstatus aktiv und beendet (d.h. reaktivierbar)
        AND c.redundant_owner_id = 1 -- keine Service Provider Vertraege
        AND c.insert_at <= stichtag
        AND (c.modified_at IS NULL OR c.modified_at > stichtag)
        AND c.valid_from <= stichtag
        AND (c.valid_to IS NULL OR c.valid_to > stichtag)
        AND c.is_production = 1
        AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
        AND bp.insert_at <= stichtag
        AND (bp.modified_at IS NULL OR bp.modified_at > stichtag)
        AND bp.valid_from <= stichtag
        AND (bp.valid_to IS NULL OR bp.valid_to > stichtag)
        AND bp.is_production = 1;

    -- Capture the number of processed records
    SET processed_records = (SELECT COUNT(*) FROM my_gcp_project.my_bq_dataset.sof_ta_bpr_instance_staging WHERE processing_date = stichtag);

    -- After successful staging, insert into the final PoolBasisprodukt table
    -- This assumes that sof_ta_bpr_instance_staging is an intermediate step before PoolBasisprodukt
    -- If this is an overwrite scenario, consider TRUNCATE + INSERT or MERGE.
    -- For now, appending to PoolBasisprodukt if records for stichtag are not expected to exist,
    -- or if PoolBasisprodukt already handles deduplication/upserts.
    INSERT INTO my_gcp_project.my_bq_dataset.PoolBasisprodukt
    SELECT * FROM my_gcp_project.my_bq_dataset.sof_ta_bpr_instance_staging WHERE processing_date = stichtag;

END;