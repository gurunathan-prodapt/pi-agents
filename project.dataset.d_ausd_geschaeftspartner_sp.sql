-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/d_ausd_geschaeftspartner.sql
-- Orchestrated by: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

-- Description: This stored procedure migrates the core data transformation logic from d_ausd_geschaeftspartner.sql
-- to BigQuery SQL. It performs several data manipulation steps to prepare business partner data.
-- It assumes that source tables (e.g., sof_ta_e_reach_gp, bpd_ta_bp_valueseg_assoc, etc.) exist in the
-- `project.dataset` with compatible schemas.
CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_geschaeftspartner_sp`(
    IN stichtag_date DATE,
    OUT record_count INT64
)
BEGIN
    DECLARE v_datum STRING;

    -- Set v_datum based on the input stichtag_date from the orchestrator script
    SET v_datum = FORMAT_DATE('%Y%m%d', stichtag_date);

    -- Step02: Truncate temporary tables
    -- Replaced Oracle DWPA_UTIL_SKRIPT.runstatement calls with BigQuery TRUNCATE TABLE
    TRUNCATE TABLE `project.dataset.sof_ta_segm_prem`;
    TRUNCATE TABLE `project.dataset.sof_ta_bpr_dn_evn`;
    TRUNCATE TABLE `project.dataset.sof_ta_bpr_dn_evn_his`;
    TRUNCATE TABLE `project.dataset.sof_ta_p_gesch_part`;
    TRUNCATE TABLE `project.dataset.sof_ta_p_dn_nutzer`;
    TRUNCATE TABLE `project.dataset.sof_ta_p_evn_empf`;

    -- Step03: erzeuge tabelle sof$ta_segm_prem
    INSERT INTO `project.dataset.sof_ta_segm_prem`
    (BP_ID, SEGMENT_ID)
    SELECT
        BP_ID,
        SEGMENT_ID
    FROM
        `project.dataset.bpd_ta_bp_valueseg_assoc`;

    -- Step04: erzeuge tabelle sof$ta_p_geschaeftspartner
    INSERT INTO `project.dataset.sof_ta_p_gesch_part`
    (CNTRCT_ID, NAMENSZUSATZ, ADRESSZUSATZ, FIRMENNAME, AKAD_TITEL, NACHNAME, VORNAME, LAND, PLZ, WOHNORT, STRASSE, KUNDE_SEGMENT_ID, PREM_SEGMENT_ID, TM_KUNDENNUMMER, MWST_KENNZEICHEN, ORGANISATIONSEINHEIT)
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
           WHEN rg.street IS NULL
           THEN
              CASE
                 WHEN rg.pobox IS NULL THEN ''
                 ELSE CONCAT('Postfach ', rg.pobox)
              END
           ELSE CONCAT(rg.street, ' ', rg.house_nr)
       END,
       CASE pr.segment_id
              WHEN 11 THEN 'SP' WHEN 12 THEN 'RV' WHEN 13 THEN 'MA'
              WHEN 14 THEN 'SO' WHEN 15 THEN 'VJ' WHEN 16 THEN 'IN'
              ELSE CAST(pr.segment_id AS STRING)
       END,
        0, -- prem_segment_id, original: pr.premium (commented out), then 0
        bp.tm_customerid,
        bp.sales_tax_freed,
        rg.address_attachment_org
    FROM
        `project.dataset.sof_ta_e_reach_gp` rg
    JOIN
        `project.dataset.sof_ta_e_business_gp` bp ON rg.bp_id = bp.bp_id
    LEFT JOIN -- (+) implies LEFT JOIN
        `project.dataset.sof_ta_segm_prem` pr ON rg.bp_id = pr.bp_id;

    -- Step05: erzeuge bpr_instance-Tabelle zur Ermittlung der Cntrct_ID der Dienstenutzer und EVN-Empfnger
    INSERT INTO `project.dataset.sof_ta_bpr_dn_evn_his`
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
        `project.dataset.pds_ta_bpri_com` bp
    WHERE
        bp.bpr_id IN (31, 2759, 2800, 2835, 2836, 2837, 2839, 2840, 3056)
        AND bp.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
        AND (bp.modified_at IS NULL OR bp.modified_at > PARSE_DATE('%Y%m%d', v_datum))
        AND bp.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
        AND bp.is_production = 1;

    INSERT INTO `project.dataset.sof_ta_bpr_dn_evn`
    (CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, CNTRCT_ID_REF, COLUMN_5VALID_TO)
    SELECT
        bp.cntrct_id,
        bp.bpr_id,
        bp.bpri_com_id, -- Mapped from bpr_instance_id in original
        bp.cntrct_id_ref,
        COALESCE(bp.valid_to, PARSE_DATE('%Y%m%d', '47121231'))
    FROM
        (
            SELECT
                bp1.*,
                MAX(COALESCE(bp1.valid_to, PARSE_DATE('%Y%m%d', '47121231'))) OVER (PARTITION BY bp1.cntrct_id, bp1.bpr_id) AS max_valid_to
            FROM
                `project.dataset.sof_ta_bpr_dn_evn_his` bp1
        ) bp
    WHERE
        COALESCE(bp.valid_to, PARSE_DATE('%Y%m%d', '47121231')) = bp.max_valid_to;

    -- Step06: erzeuge tabelle sof$ta_p_dienstenutzer
    INSERT INTO `project.dataset.sof_ta_p_dn_nutzer`
    (CNTRCT_ID, NAMENSZUSATZ, ADRESSZUSATZ, FIRMENNAME, AKAD_TITEL, NACHNAME, VORNAME, LAND, PLZ, WOHNORT, STRASSE, ORGANISATIONSEINHEIT, MWST_KENNZEICHEN)
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
           WHEN dn.street IS NULL
           THEN
              CASE
                 WHEN dn.pobox IS NULL THEN ''
                 ELSE CONCAT('Postfach ', dn.pobox)
              END
           ELSE CONCAT(dn.street, ' ', dn.house_nr)
        END,
        dn.address_attachment_org,
        bp.sales_tax_freed
    FROM
        `project.dataset.sof_ta_e_reach_dn` dn
    JOIN
        `project.dataset.sof_ta_e_business_dn` bp ON dn.bp_id = bp.bp_id
    JOIN
        `project.dataset.sof_ta_bpr_dn_evn` bi ON dn.bpr_inst_srvusr_id = bi.bpr_instance_id;

    -- Step07: erzeuge tabelle sof$ta_p_evn_empfaenger
    INSERT INTO `project.dataset.sof_ta_p_evn_empf`
    (CNTRCT_ID, NAMENSZUSATZ, ADRESSZUSATZ, FIRMENNAME, AKAD_TITEL, NACHNAME, VORNAME, LAND, PLZ, WOHNORT, STRASSE, ORGANISATIONSEINHEIT, MWST_KENNZEICHEN)
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
           WHEN ev.street IS NULL
           THEN
              CASE
                 WHEN ev.pobox IS NULL THEN ''
                 ELSE CONCAT('Postfach ', ev.pobox)
              END
           ELSE CONCAT(ev.street, ' ', ev.house_nr)
        END,
        ev.address_attachment_org,
        bp.sales_tax_freed
    FROM
        `project.dataset.sof_ta_e_reach_ev` ev
    JOIN
        `project.dataset.sof_ta_e_business_ev` bp ON ev.bp_id = bp.bp_id
    JOIN
        `project.dataset.sof_ta_bpr_dn_evn` bi ON ev.bpr_inst_evnrec_id = bi.bpr_instance_id;

    -- Step08: Capture total record count
    -- Assuming the count from sof_ta_p_gesch_part is the desired output record count.
    -- If another table's count is needed, this should be adjusted.
    SELECT COUNT(*) INTO record_count FROM `project.dataset.sof_ta_p_gesch_part`;

END;