-- BigQuery Stored Procedure for d_ausd_bp_ta_bpr_instance.sql
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_instance.sql
-- This procedure migrates the core data manipulation logic from the original Oracle SQL script.

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.d_ausd_bp_ta_bpr_instance`(
    p_EintragsNr STRING,
    p_JobKennung STRING,
    p_Stichtag STRING, -- Input as 'DDMMYYYY' from main procedure
    p_wiederanlaufWert STRING,
    v_datum_heute DATE,
    v_datum_gestern DATE
)
OPTIONS(
    description="Migrated core data processing logic for basisprodukt instances."
)
BEGIN
    -- Declare local variables if needed, for now using direct parameter usage.
    DECLARE v_stichtag_date DATE;

    -- Convert p_Stichtag from STRING 'DDMMYYYY' to DATE
    SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);

    -- NOTE: The original Oracle script used a dynamically derived v_datum.
    -- Here, we use p_Stichtag which should be the primary business date for the run.
    -- The original Oracle script used isbert_schema.dwtk_meldungen to determine v_datum.
    -- If this logic is needed, it must be re-implemented. For now, we assume p_Stichtag
    -- directly provides the required date for filtering.

    -- ========================= Step01 ==================================
    -- Truncate the target table.
    -- Replaces 'TRUNCATE TABLE sof$ta_bpr_instance REUSE STORAGE'
    -- Assuming `sof_ta_bpr_instance` is the target table equivalent to `sof$ta_bpr_instance`.
    -- If 'PoolBasisprodukt' is the actual target table, replace 'sof_ta_bpr_instance' below.
    DELETE FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance` WHERE TRUE;

    -- ========================= Step03 ==================================
    -- Insert data into the target table.
    -- Replaces the Oracle INSERT statement.
    INSERT INTO `your_project_id.your_dataset_id.sof_ta_bpr_instance`
    (CNTRCT_ID,
      BPR_ID,
      BPR_INSTANCE_ID,
      ICCID,
      IMSI_MCC,
      IMSI_MNC,
      IMSI_HLR,
      IMSI_SI,
      CNTRCT_ID_REF)
    SELECT
            bp.cntrct_id,
            bp.bpr_id,
            bp.bpri_com_id  AS bpr_instance_id,
            -- Oracle CONCAT (||) replaced with BigQuery CONCAT or ||
            CONCAT(bp.iccid_mi,'-',bp.iccid_ii,'-',bp.iccid_iai,'-',bp.iccid_nr,'-',bp.iccid_cd) as iccid,
            bp.imsi_mcc,
            bp.imsi_mnc,
            bp.imsi_hlr,
            bp.imsi_si,
            bp.cntrct_id_ref
    FROM    `your_project_id.your_dataset_id.cds_ta_cntrct` c -- Assuming 'cds$ta_cntrct' maps to 'cds_ta_cntrct'
    JOIN    `your_project_id.your_dataset_id.pds_ta_bpri_com` bp -- Assuming 'pds$ta_bpri_com' maps to 'pds_ta_bpri_com'
    ON      c.cntrct_id = bp.cntrct_id
    WHERE   c.cntrct_st IN (5, 6)
      AND   c.redundant_owner_id = 1
      -- Oracle TO_DATE('&v_datum','YYYYMMDD') replaced with v_stichtag_date
      AND   c.insert_at <= v_stichtag_date
      AND   (   c.modified_at IS NULL
             OR c.modified_at > v_stichtag_date)
      AND   c.valid_from <= v_stichtag_date
      AND   (   c.valid_to IS NULL
             OR c.valid_to > v_stichtag_date)
      AND   c.is_production = 1
      AND   (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
      AND   bp.insert_at <= v_stichtag_date
      AND   (   bp.modified_at IS NULL
             OR bp.modified_at > v_stichtag_date)
      AND   bp.valid_from <= v_stichtag_date
      AND   (   bp.valid_to IS NULL
             OR bp.valid_to > v_stichtag_date)
      AND   bp.is_production = 1;

    -- COMMIT is implicit in BigQuery DML operations within a script/procedure.

END;