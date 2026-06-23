-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_geschaeftspartner.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh

-- This script contains translated Oracle SQL to BigQuery SQL.
-- Placeholder for dynamic date parameter: @p_stichtag_yyyymmdd

-- ========================= Step00 ==================================
-- Original DEFINE and COLUMN commands are removed.
-- The value for v_datum is now passed as @p_stichtag_yyyymmdd
-- from the orchestrating PySpark script.

-- ========================= Step01 ==================================
-- Original DESC statements are removed as they are for schema inspection.

-- ========================= Step02 ==================================
-- Truncate target tables for restart scenario.
TRUNCATE TABLE isbert_target_ds.sof_ta_segm_prem;
TRUNCATE TABLE isbert_target_ds.sof_ta_bpr_dn_evn;
TRUNCATE TABLE isbert_target_ds.sof_ta_bpr_dn_evn_his;
TRUNCATE TABLE isbert_target_ds.sof_ta_p_gesch_part;
TRUNCATE TABLE isbert_target_ds.sof_ta_p_dn_nutzer;
TRUNCATE TABLE isbert_target_ds.sof_ta_p_evn_empf;

-- ========================= Step03 ==================================
INSERT INTO isbert_target_ds.sof_ta_segm_prem
(BP_ID, SEGMENT_ID)
SELECT
    BP_ID,
    SEGMENT_ID
FROM
    isbert_source_ds.bpd_ta_bp_valueseg_assoc;

-- ========================= Step04 ==================================
INSERT INTO isbert_target_ds.sof_ta_p_gesch_part
(
    CNTRCT_ID, NAMENSZUSATZ, ADRESSZUSATZ, FIRMENNAME, AKAD_TITEL,
    NACHNAME, VORNAME, LAND, PLZ, WOHNORT, STRASSE, KUNDE_SEGMENT_ID,
    PREM_SEGMENT_ID, TM_KUNDENNUMMER, MWST_KENNZEICHEN, ORGANISATIONSEINHEIT
)
SELECT
    rg.cntrct_cp2_id,
    rg.for_the_attention_of,
    rg.address_attachment,
    COALESCE(rg.corp_unit, bp.organisation_name),
    CASE WHEN rg.surname_s IS NULL THEN bp.title ELSE '' END,
    COALESCE(rg.surname_s, bp.surname),
    COALESCE(rg.first_name_g, bp.first_name),
    rg.land_sd,
    rg.zip_code,
    rg.city,
    CASE
        WHEN rg.street IS NULL THEN
            CASE
                WHEN rg.pobox IS NULL THEN ''
                ELSE CONCAT('Postfach ', rg.pobox)
            END
        ELSE CONCAT(rg.street, ' ', rg.house_nr)
    END,
    CASE
        pr.segment_id
        WHEN 11 THEN 'SP'
        WHEN 12 THEN 'RV'
        WHEN 13 THEN 'MA'
        WHEN 14 THEN 'SO'
        WHEN 15 THEN 'VJ'
        WHEN 16 THEN 'IN'
        ELSE CAST(pr.segment_id AS STRING)
    END,
    0, -- PREM_SEGMENT_ID, original comment: FD (21.05.2008), was 0
    bp.tm_customerid,
    bp.sales_tax_freed,
    rg.address_attachment_org
FROM
    isbert_source_ds.sof_ta_e_reach_gp AS rg
JOIN
    isbert_source_ds.sof_ta_e_business_gp AS bp ON rg.bp_id = bp.bp_id
LEFT JOIN
    isbert_target_ds.sof_ta_segm_prem AS pr ON rg.bp_id = pr.bp_id;

-- ========================= Step05 ==================================
INSERT INTO isbert_target_ds.sof_ta_bpr_dn_evn_his
(CNTRCT_ID, BPR_ID, BPRI_COM_ID, CNTRCT_ID_REF, VALID_FROM, VALID_TO, MODIFIED_AT, INSERT_AT)
SELECT
    bp.cntrct_id,
    bp.bpr_id,
    bp.bpri_com_id,
    bp.cntrct_id_ref,
    bp.valid_from,
    bp.valid_to,
    bp.modified_at,
    bp.insert_at
FROM
    isbert_source_ds.pds_ta_bpri_com AS bp
WHERE
    bp.bpr_id IN (31, 2759, 2800, 2835, 2836, 2837, 2839, 2840, 3056)
    AND bp.insert_at <= PARSE_DATE('%Y%m%d', @p_stichtag_yyyymmdd)
    AND (bp.modified_at IS NULL OR bp.modified_at > PARSE_DATE('%Y%m%d', @p_stichtag_yyyymmdd))
    AND bp.valid_from <= PARSE_DATE('%Y%m%d', @p_stichtag_yyyymmdd)
    AND bp.is_production = 1;

INSERT INTO isbert_target_ds.sof_ta_bpr_dn_evn
(CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, CNTRCT_ID_REF, COLUMN_5VALID_TO)
SELECT
    bp.cntrct_id,
    bp.bpr_id,
    bp.bpri_com_id AS bpr_instance_id,
    bp.cntrct_id_ref,
    COALESCE(bp.valid_to, PARSE_DATE('%Y%m%d', '47121231')) AS valid_to
FROM
    (
        SELECT
            bp1.*,
            MAX(COALESCE(bp1.valid_to, PARSE_DATE('%Y%m%d', '47121231'))) OVER (PARTITION BY bp1.cntrct_id, bp1.bpr_id) AS max_valid_to
        FROM
            isbert_target_ds.sof_ta_bpr_dn_evn_his AS bp1
    ) AS bp
WHERE
    COALESCE(bp.valid_to, PARSE_DATE('%Y%m%d', '47121231')) = bp.max_valid_to;

-- ========================= Step06 ==================================
INSERT INTO isbert_target_ds.sof_ta_p_dn_nutzer
(
    CNTRCT_ID, NAMENSZUSATZ, ADRESSZUSATZ, FIRMENNAME, AKAD_TITEL,
    NACHNAME, VORNAME, LAND, PLZ, WOHNORT, STRASSE, ORGANISATIONSEINHEIT, MWST_KENNZEICHEN
)
SELECT
    bi.cntrct_id,
    dn.for_the_attention_of,
    dn.address_attachment,
    COALESCE(dn.corp_unit, bp.organisation_name),
    CASE WHEN dn.surname_s IS NULL THEN bp.title ELSE '' END,
    COALESCE(dn.surname_s, bp.surname),
    COALESCE(dn.first_name_g, bp.first_name),
    dn.land_sd,
    dn.zip_code,
    dn.city,
    CASE
        WHEN dn.street IS NULL THEN
            CASE
                WHEN dn.pobox IS NULL THEN ''
                ELSE CONCAT('Postfach ', dn.pobox)
            END
        ELSE CONCAT(dn.street, ' ', dn.house_nr)
    END,
    dn.address_attachment_org,
    bp.sales_tax_freed
FROM
    isbert_source_ds.sof_ta_e_reach_dn AS dn
JOIN
    isbert_source_ds.sof_ta_e_business_dn AS bp ON dn.bp_id = bp.bp_id
JOIN
    isbert_target_ds.sof_ta_bpr_dn_evn AS bi ON dn.bpr_inst_srvusr_id = bi.bpr_instance_id;

-- ========================= Step07 ==================================
INSERT INTO isbert_target_ds.sof_ta_p_evn_empf
(
    CNTRCT_ID, NAMENSZUSATZ, ADRESSZUSATZ, FIRMENNAME, AKAD_TITEL,
    NACHNAME, VORNAME, LAND, PLZ, WOHNORT, STRASSE, ORGANISATIONSEINHEIT, MWST_KENNZEICHEN
)
SELECT
    bi.cntrct_id,
    ev.for_the_attention_of,
    ev.address_attachment,
    COALESCE(ev.corp_unit, bp.organisation_name),
    CASE WHEN ev.surname_s IS NULL THEN bp.title ELSE '' END,
    COALESCE(ev.surname_s, bp.surname),
    COALESCE(ev.first_name_g, bp.first_name),
    ev.land_sd,
    ev.zip_code,
    ev.city,
    CASE
        WHEN ev.street IS NULL THEN
            CASE
                WHEN ev.pobox IS NULL THEN ''
                ELSE CONCAT('Postfach ', ev.pobox)
            END
        ELSE CONCAT(ev.street, ' ', ev.house_nr)
    END,
    ev.address_attachment_org,
    bp.sales_tax_freed
FROM
    isbert_source_ds.sof_ta_e_reach_ev AS ev
JOIN
    isbert_source_ds.sof_ta_e_business_ev AS bp ON ev.bp_id = bp.bp_id
JOIN
    isbert_target_ds.sof_ta_bpr_dn_evn AS bi ON ev.bpr_inst_evnrec_id = bi.bpr_instance_id;

-- ========================= Step08 ==================================
-- The commented-out TRUNCATE statements are not executed in the original.
-- Retaining them commented to indicate the original script's intent for cleanup.
-- TRUNCATE TABLE isbert_target_ds.sof_ta_segm_prem;
-- TRUNCATE TABLE isbert_target_ds.sof_ta_bpr_dn_evn;