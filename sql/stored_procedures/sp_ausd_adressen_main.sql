-- BigQuery Stored Procedure for k_ausd_adressen.ksh
-- Migrates logic from d_ausd_adressen.sql
CREATE OR REPLACE PROCEDURE `PROJECT_ID.dataset.sp_ausd_adressen_main`(
    p_job_kennung STRING,
    p_eintrags_nr INT64,
    p_stichtag_str STRING, -- YYYYMMDD format
    p_wiederanlauf_wert STRING
)
BEGIN
    DECLARE v_datum_date DATE;
    DECLARE v_job_status STRING DEFAULT 'SUCCESS';
    DECLARE v_error_message STRING;
    DECLARE v_record_count INT64;

    -- Parameter validation and date parsing
    BEGIN
        SET v_datum_date = PARSE_DATE('%Y%m%d', p_stichtag_str);
    EXCEPTION WHEN ERROR THEN
        SET v_job_status = 'FAILED';
        SET v_error_message = 'Invalid date format for p_stichtag_str. Expected YYYYMMDD.';
        INSERT INTO `PROJECT_ID.metrics.job_log`
        (job_id, entry_number, key_date, restart_value, start_timestamp, end_timestamp, status, error_message)
        VALUES
        (p_job_kennung, p_eintrags_nr, NULL, p_wiederanlauf_wert, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), v_job_status, v_error_message);
        RAISE USING MESSAGE = v_error_message;
    END;

    -- Step 01: Truncate temporary tables
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_gp`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_re`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_ev`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_dn`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_gp_nodp`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_re_nodp`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_ev_nodp`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_dn_nodp`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_reachability`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_business_pt`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_country`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_country_desc`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_laender_kng`;
    TRUNCATE TABLE `PROJECT_ID.target.sof_ta_e_reach_gp`;
    TRUNCATE TABLE `PROJECT_ID.target.sof_ta_e_reach_re`;
    TRUNCATE TABLE `PROJECT_ID.target.sof_ta_e_reach_dn`;
    TRUNCATE TABLE `PROJECT_ID.target.sof_ta_e_reach_ev`;
    TRUNCATE TABLE `PROJECT_ID.target.sof_ta_e_business_gp`;
    TRUNCATE TABLE `PROJECT_ID.target.sof_ta_e_business_re`;
    TRUNCATE TABLE `PROJECT_ID.target.sof_ta_e_business_dn`;
    TRUNCATE TABLE `PROJECT_ID.target.sof_ta_e_business_ev`;
    TRUNCATE TABLE `PROJECT_ID.target.sof_ta_e_regulierer`;


    -- Step 02a: Populate sof_ta_bp_ref_gp
    INSERT INTO `PROJECT_ID.staging.sof_ta_bp_ref_gp`
    (bp_id, reachability_id, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id, bpr_inst_srvusr_id)
    SELECT
        bpr.bp_id,
        bpr.reachability_id,
        bpr.cntrct_cp2_id,
        bpr.inv_def_invrec_id,
        bpr.bpr_inst_evnrec_id,
        bpr.bpr_inst_srvusr_id
    FROM
        `PROJECT_ID.raw.cds_ta_bp_ref` AS bpr
    WHERE
        bpr.insert_at <= TIMESTAMP(v_datum_date)
        AND (bpr.modified_at IS NULL OR bpr.modified_at > TIMESTAMP(v_datum_date))
        AND bpr.valid_from <= TIMESTAMP(v_datum_date)
        AND (bpr.valid_to IS NULL OR bpr.valid_to > TIMESTAMP(v_datum_date))
        AND bpr.is_production = 1
        AND bpr.bp_ref_ty = 4
        AND bpr.address_ref_ty = 6;

    -- Step 02b: Populate sof_ta_bp_ref_re
    INSERT INTO `PROJECT_ID.staging.sof_ta_bp_ref_re`
    (bp_id, reachability_id, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id, bpr_inst_srvusr_id)
    SELECT
        bpr.bp_id,
        bpr.reachability_id,
        bpr.cntrct_cp2_id,
        bpr.inv_def_invrec_id,
        bpr.bpr_inst_evnrec_id,
        bpr.bpr_inst_srvusr_id
    FROM
        `PROJECT_ID.raw.cds_ta_bp_ref` AS bpr
    WHERE
        bpr.insert_at <= TIMESTAMP(v_datum_date)
        AND (bpr.modified_at IS NULL OR bpr.modified_at > TIMESTAMP(v_datum_date))
        AND bpr.valid_from <= TIMESTAMP(v_datum_date)
        AND (bpr.valid_to IS NULL OR bpr.valid_to > TIMESTAMP(v_datum_date))
        AND bpr.is_production = 1
        AND bpr.bp_ref_ty = 1
        AND bpr.address_ref_ty = 5
    UNION ALL
    SELECT
        id.rdndnt_cp2_bp_id AS bp_id,
        id.rdndnt_cp2_reachability_id AS reachability_id,
        NULL AS cntrct_cp2_id,
        id.inv_definition_id AS inv_def_invrec_id,
        NULL AS bpr_inst_evnrec_id,
        NULL AS bpr_inst_srvusr_id
    FROM
        `PROJECT_ID.raw.cds_ta_inv_definition` AS id
    WHERE
        id.insert_at <= TIMESTAMP(v_datum_date)
        AND (id.modified_at IS NULL OR id.modified_at > TIMESTAMP(v_datum_date))
        AND id.valid_from <= TIMESTAMP(v_datum_date)
        AND (id.valid_to IS NULL OR id.valid_to > TIMESTAMP(v_datum_date))
        AND id.is_production = 1
        AND id.rdndant_invrec = 0;

    -- Step 02c: Populate sof_ta_bp_ref_ev
    INSERT INTO `PROJECT_ID.staging.sof_ta_bp_ref_ev`
    (bp_id, reachability_id, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id, bpr_inst_srvusr_id)
    SELECT
        bpr.bp_id,
        bpr.reachability_id,
        bpr.cntrct_cp2_id,
        bpr.inv_def_invrec_id,
        bpr.bpr_inst_evnrec_id,
        bpr.bpr_inst_srvusr_id
    FROM
        `PROJECT_ID.raw.cds_ta_bp_ref` AS bpr
    WHERE
        bpr.insert_at <= TIMESTAMP(v_datum_date)
        AND (bpr.modified_at IS NULL OR bpr.modified_at > TIMESTAMP(v_datum_date))
        AND bpr.valid_from <= TIMESTAMP(v_datum_date)
        AND (bpr.valid_to IS NULL OR bpr.valid_to > TIMESTAMP(v_datum_date))
        AND bpr.is_production = 1
        AND bpr.bp_ref_ty = 1
        AND bpr.address_ref_ty = 7;

    -- Step 02d: Populate sof_ta_bp_ref_dn
    INSERT INTO `PROJECT_ID.staging.sof_ta_bp_ref_dn`
    (bp_id, reachability_id, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id, bpr_inst_srvusr_id)
    SELECT
        bpr.bp_id,
        bpr.reachability_id,
        bpr.cntrct_cp2_id,
        bpr.inv_def_invrec_id,
        bpr.bpr_inst_evnrec_id,
        bpr.bpr_inst_srvusr_id
    FROM
        `PROJECT_ID.raw.cds_ta_bp_ref` AS bpr
    WHERE
        bpr.insert_at <= TIMESTAMP(v_datum_date)
        AND (bpr.modified_at IS NULL OR bpr.modified_at > TIMESTAMP(v_datum_date))
        AND bpr.valid_from <= TIMESTAMP(v_datum_date)
        AND (bpr.valid_to IS NULL OR bpr.valid_to > TIMESTAMP(v_datum_date))
        AND bpr.is_production = 1
        AND bpr.bp_ref_ty = 1
        AND bpr.address_ref_ty = 8;

    -- Step 03a: Populate sof_ta_country
    INSERT INTO `PROJECT_ID.staging.sof_ta_country`
    (country_code, description_id, parent_country_code, eu_indicator, sap_code, corr_code, valid)
    SELECT
        country.country_code,
        country.description_id,
        country.parent_country_code,
        country.eu_indicator,
        country.sap_code,
        country.corr_code,
        country.valid
    FROM
        `PROJECT_ID.raw.glv_ta_country` AS country;

    -- Step 03b: Populate sof_ta_country_desc
    INSERT INTO `PROJECT_ID.staging.sof_ta_country_desc`
    (description_id, language, short_description, description, long_description)
    SELECT
        des.description_id,
        des.language,
        des.short_description,
        des.description,
        des.long_description
    FROM
        `PROJECT_ID.raw.glv_ta_description` AS des;

    -- Step 03c: Populate sof_ta_laender_kng
    INSERT INTO `PROJECT_ID.staging.sof_ta_laender_kng`
    (country_code, description_id, language, short_description, description, long_description)
    SELECT
        co.country_code,
        de.description_id,
        de.language,
        de.short_description,
        de.description,
        de.long_description
    FROM
        `PROJECT_ID.staging.sof_ta_country` AS co
    JOIN
        `PROJECT_ID.staging.sof_ta_country_desc` AS de
    ON
        co.description_id = de.description_id
    WHERE
        co.valid = 1;

    -- Step 03e: Populate sof_ta_reachability
    INSERT INTO `PROJECT_ID.staging.sof_ta_reachability`
    (bp_id, reachability_id, obj_version, country_code, for_the_attention_of, address_attachment,
     address_attachment_org, corp_unit, surname_s, first_name_g, zip_code, city, pobox, street,
     house_nr, public_area_a, private_area_p, corp_unit_ou1, address_line_1, address_line_2,
     reachable_from, reachable_thru)
    SELECT
        re.bp_id,
        re.reachability_id,
        re.obj_version,
        re.country_code,
        re.for_the_attention_of,
        re.address_attachment,
        re.address_attachment_org,
        re.corp_unit,
        re.surname_s,
        re.first_name_g,
        re.zip_code,
        re.city,
        re.pobox,
        re.street,
        re.house_nr,
        re.public_area_a,
        re.private_area_p,
        re.corp_unit_ou1,
        re.address_line_1,
        re.address_line_2,
        re.reachable_from,
        re.reachable_thru
    FROM
        `PROJECT_ID.raw.bpd_ta_reachability` AS re
    WHERE
        re.insert_at <= TIMESTAMP(v_datum_date)
        AND (re.modified_at IS NULL OR re.modified_at > TIMESTAMP(v_datum_date))
        AND re.valid_from <= TIMESTAMP(v_datum_date)
        AND (re.valid_to IS NULL OR re.valid_to > TIMESTAMP(v_datum_date))
        AND re.is_production = 1;

    -- Step 03f: Populate sof_ta_e_reach_gp
    INSERT INTO `PROJECT_ID.target.sof_ta_e_reach_gp`
    (bp_id, reachability_id, obj_version, country_code, for_the_attention_of, address_attachment,
     address_attachment_org, corp_unit, surname_s, first_name_g, zip_code, city, pobox, street,
     house_nr, public_area_a, private_area_p, corp_unit_ou1, address_line_1, address_line_2,
     reachable_from, reachable_thru, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id,
     bpr_inst_srvusr_id, land_sd)
    SELECT
        re.bp_id,
        re.reachability_id,
        re.obj_version,
        re.country_code,
        re.for_the_attention_of,
        re.address_attachment,
        re.address_attachment_org,
        re.corp_unit,
        re.surname_s,
        re.first_name_g,
        re.zip_code,
        re.city,
        re.pobox,
        re.street,
        re.house_nr,
        re.public_area_a,
        re.private_area_p,
        re.corp_unit_ou1,
        re.address_line_1,
        re.address_line_2,
        re.reachable_from,
        re.reachable_thru,
        br.cntrct_cp2_id,
        br.inv_def_invrec_id,
        br.bpr_inst_evnrec_id,
        br.bpr_inst_srvusr_id,
        SUBSTR(lk.short_description, 1, 3) AS land_sd
    FROM
        `PROJECT_ID.staging.sof_ta_bp_ref_gp` AS br
    JOIN
        `PROJECT_ID.staging.sof_ta_reachability` AS re
    ON
        br.bp_id = re.bp_id AND br.reachability_id = re.reachability_id
    LEFT JOIN
        `PROJECT_ID.staging.sof_ta_laender_kng` AS lk
    ON
        re.country_code = lk.country_code;

    -- Step 03g: Populate sof_ta_e_reach_re
    INSERT INTO `PROJECT_ID.target.sof_ta_e_reach_re`
    (bp_id, reachability_id, obj_version, country_code, for_the_attention_of, address_attachment,
     address_attachment_org, corp_unit, surname_s, first_name_g, zip_code, city, pobox, street,
     house_nr, public_area_a, private_area_p, corp_unit_ou1, address_line_1, address_line_2,
     reachable_from, reachable_thru, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id,
     bpr_inst_srvusr_id, land_sd)
    SELECT
        re.bp_id,
        re.reachability_id,
        re.obj_version,
        re.country_code,
        re.for_the_attention_of,
        re.address_attachment,
        re.address_attachment_org,
        re.corp_unit,
        re.surname_s,
        re.first_name_g,
        re.zip_code,
        re.city,
        re.pobox,
        re.street,
        re.house_nr,
        re.public_area_a,
        re.private_area_p,
        re.corp_unit_ou1,
        re.address_line_1,
        re.address_line_2,
        re.reachable_from,
        re.reachable_thru,
        br.cntrct_cp2_id,
        br.inv_def_invrec_id,
        br.bpr_inst_evnrec_id,
        br.bpr_inst_srvusr_id,
        SUBSTR(lk.short_description, 1, 3) AS land_sd
    FROM
        `PROJECT_ID.staging.sof_ta_bp_ref_re` AS br
    JOIN
        `PROJECT_ID.staging.sof_ta_reachability` AS re
    ON
        br.bp_id = re.bp_id AND br.reachability_id = re.reachability_id
    LEFT JOIN
        `PROJECT_ID.staging.sof_ta_laender_kng` AS lk
    ON
        re.country_code = lk.country_code;

    -- Step 03h: Populate sof_ta_e_reach_ev
    INSERT INTO `PROJECT_ID.target.sof_ta_e_reach_ev`
    (bp_id, reachability_id, obj_version, country_code, for_the_attention_of, address_attachment,
     address_attachment_org, corp_unit, surname_s, first_name_g, zip_code, city, pobox, street,
     house_nr, public_area_a, private_area_p, corp_unit_ou1, address_line_1, address_line_2,
     reachable_from, reachable_thru, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id,
     bpr_inst_srvusr_id, land_sd)
    SELECT
        re.bp_id,
        re.reachability_id,
        re.obj_version,
        re.country_code,
        re.for_the_attention_of,
        re.address_attachment,
        re.address_attachment_org,
        re.corp_unit,
        re.surname_s,
        re.first_name_g,
        re.zip_code,
        re.city,
        re.pobox,
        re.street,
        re.house_nr,
        re.public_area_a,
        re.private_area_p,
        re.corp_unit_ou1,
        re.address_line_1,
        re.address_line_2,
        re.reachable_from,
        re.reachable_thru,
        br.cntrct_cp2_id,
        br.inv_def_invrec_id,
        br.bpr_inst_evnrec_id,
        br.bpr_inst_srvusr_id,
        SUBSTR(lk.short_description, 1, 3) AS land_sd
    FROM
        `PROJECT_ID.staging.sof_ta_bp_ref_ev` AS br
    JOIN
        `PROJECT_ID.staging.sof_ta_reachability` AS re
    ON
        br.bp_id = re.bp_id AND br.reachability_id = re.reachability_id
    LEFT JOIN
        `PROJECT_ID.staging.sof_ta_laender_kng` AS lk
    ON
        re.country_code = lk.country_code;

    -- Step 03i: Populate sof_ta_e_reach_dn
    INSERT INTO `PROJECT_ID.target.sof_ta_e_reach_dn`
    (bp_id, reachability_id, obj_version, country_code, for_the_attention_of, address_attachment,
     address_attachment_org, corp_unit, surname_s, first_name_g, zip_code, city, pobox, street,
     house_nr, public_area_a, private_area_p, corp_unit_ou1, address_line_1, address_line_2,
     reachable_from, reachable_thru, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id,
     bpr_inst_srvusr_id, land_sd)
    SELECT
        re.bp_id,
        re.reachability_id,
        re.obj_version,
        re.country_code,
        re.for_the_attention_of,
        re.address_attachment,
        re.address_attachment_org,
        re.corp_unit,
        re.surname_s,
        re.first_name_g,
        re.zip_code,
        re.city,
        re.pobox,
        re.street,
        re.house_nr,
        re.public_area_a,
        re.private_area_p,
        re.corp_unit_ou1,
        re.address_line_1,
        re.address_line_2,
        re.reachable_from,
        re.reachable_thru,
        br.cntrct_cp2_id,
        br.inv_def_invrec_id,
        br.bpr_inst_evnrec_id,
        br.bpr_inst_srvusr_id,
        SUBSTR(lk.short_description, 1, 3) AS land_sd
    FROM
        `PROJECT_ID.staging.sof_ta_bp_ref_dn` AS br
    JOIN
        `PROJECT_ID.staging.sof_ta_reachability` AS re
    ON
        br.bp_id = re.bp_id AND br.reachability_id = re.reachability_id
    LEFT JOIN
        `PROJECT_ID.staging.sof_ta_laender_kng` AS lk
    ON
        re.country_code = lk.country_code;

    -- Step 03j: Cleanup intermediate tables
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_reachability`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_country`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_country_desc`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_laender_kng`;

    -- Step 04a: Populate sof_ta_business_pt
    INSERT INTO `PROJECT_ID.staging.sof_ta_business_pt`
    (bp_id, organisation_name, title, surname, first_name, sales_tax_freed, tm_customerid)
    SELECT
        bp.bp_id,
        bp.organisation_name,
        bp.title,
        bp.surname,
        bp.first_name,
        bp.sales_tax_freed,
        bp.tm_customerid
    FROM
        `PROJECT_ID.raw.bpd_ta_business_partner` AS bp
    WHERE
        bp.insert_at <= TIMESTAMP(v_datum_date)
        AND (bp.modified_at IS NULL OR bp.modified_at > TIMESTAMP(v_datum_date));

    -- Step 04b: Populate sof_ta_bp_ref_gp_nodp
    INSERT INTO `PROJECT_ID.staging.sof_ta_bp_ref_gp_nodp`
    (bp_id)
    SELECT DISTINCT
        bp_id
    FROM
        `PROJECT_ID.staging.sof_ta_bp_ref_gp`;

    -- Step 04b: Populate sof_ta_e_business_gp
    INSERT INTO `PROJECT_ID.target.sof_ta_e_business_gp`
    (bp_id, organisation_name, title, surname, first_name, sales_tax_freed, tm_customerid)
    SELECT
        bp.bp_id,
        bp.organisation_name,
        bp.title,
        bp.surname,
        bp.first_name,
        bp.sales_tax_freed,
        bp.tm_customerid
    FROM
        `PROJECT_ID.staging.sof_ta_bp_ref_gp_nodp` AS br
    JOIN
        `PROJECT_ID.staging.sof_ta_business_pt` AS bp
    ON
        br.bp_id = bp.bp_id;

    -- Step 04c: Cleanup intermediate tables
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_gp_nodp`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_gp`;

    -- Step 04d: Populate sof_ta_bp_ref_re_nodp
    INSERT INTO `PROJECT_ID.staging.sof_ta_bp_ref_re_nodp`
    (bp_id)
    SELECT DISTINCT
        bp_id
    FROM
        `PROJECT_ID.staging.sof_ta_bp_ref_re`;

    -- Step 04d: Populate sof_ta_e_business_re
    INSERT INTO `PROJECT_ID.target.sof_ta_e_business_re`
    (bp_id, organisation_name, title, surname, first_name, sales_tax_freed, tm_customerid)
    SELECT
        bp.bp_id,
        bp.organisation_name,
        bp.title,
        bp.surname,
        bp.first_name,
        bp.sales_tax_freed,
        bp.tm_customerid
    FROM
        `PROJECT_ID.staging.sof_ta_bp_ref_re_nodp` AS br
    JOIN
        `PROJECT_ID.staging.sof_ta_business_pt` AS bp
    ON
        br.bp_id = bp.bp_id;

    -- Step 04e: Cleanup intermediate tables
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_re_nodp`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_re`;

    -- Step 04f: Populate sof_ta_bp_ref_ev_nodp
    INSERT INTO `PROJECT_ID.staging.sof_ta_bp_ref_ev_nodp`
    (bp_id)
    SELECT DISTINCT
        bp_id
    FROM
        `PROJECT_ID.staging.sof_ta_bp_ref_ev`;

    -- Step 04f: Populate sof_ta_e_business_ev
    INSERT INTO `PROJECT_ID.target.sof_ta_e_business_ev`
    (bp_id, organisation_name, title, surname, first_name, sales_tax_freed, tm_customerid)
    SELECT
        bp.bp_id,
        bp.organisation_name,
        bp.title,
        bp.surname,
        bp.first_name,
        bp.sales_tax_freed,
        bp.tm_customerid
    FROM
        `PROJECT_ID.staging.sof_ta_bp_ref_ev_nodp` AS br
    JOIN
        `PROJECT_ID.staging.sof_ta_business_pt` AS bp
    ON
        br.bp_id = bp.bp_id;

    -- Step 04g: Cleanup intermediate tables
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_ev_nodp`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_ev`;

    -- Step 04h: Populate sof_ta_bp_ref_dn_nodp
    INSERT INTO `PROJECT_ID.staging.sof_ta_bp_ref_dn_nodp`
    (bp_id)
    SELECT DISTINCT
        bp_id
    FROM
        `PROJECT_ID.staging.sof_ta_bp_ref_dn`;

    -- Step 04h: Populate sof_ta_e_business_dn
    INSERT INTO `PROJECT_ID.target.sof_ta_e_business_dn`
    (bp_id, organisation_name, title, surname, first_name, sales_tax_freed, tm_customerid)
    SELECT
        bp.bp_id,
        bp.organisation_name,
        bp.title,
        bp.surname,
        bp.first_name,
        bp.sales_tax_freed,
        bp.tm_customerid
    FROM
        `PROJECT_ID.staging.sof_ta_bp_ref_dn_nodp` AS br
    JOIN
        `PROJECT_ID.staging.sof_ta_business_pt` AS bp
    ON
        br.bp_id = bp.bp_id;

    -- Step 04i: Cleanup intermediate tables
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_business_pt`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_dn_nodp`;
    TRUNCATE TABLE `PROJECT_ID.staging.sof_ta_bp_ref_dn`;

    -- Step 05: Populate sof_ta_e_regulierer
    INSERT INTO `PROJECT_ID.target.sof_ta_e_regulierer`
    (inv_def_mopref_id, mop_bp_id, means_of_payment_id)
    SELECT
        bpr.inv_def_mopref_id,
        bpr.mop_bp_id,
        bpr.means_of_payment_id
    FROM
        `PROJECT_ID.raw.cds_ta_bp_ref` AS bpr
    WHERE
        bpr.insert_at <= TIMESTAMP(v_datum_date)
        AND (bpr.modified_at IS NULL OR bpr.modified_at > TIMESTAMP(v_datum_date))
        AND bpr.valid_from <= TIMESTAMP(v_datum_date)
        AND (bpr.valid_to IS NULL OR bpr.valid_to > TIMESTAMP(v_datum_date))
        AND bpr.is_production = 1
        AND bpr.bp_ref_ty = 2
        AND bpr.mop_ref_ty = 1;

    -- Log successful execution (example for one target table)
    SET v_record_count = (SELECT COUNT(*) FROM `PROJECT_ID.target.sof_ta_e_regulierer`);
    INSERT INTO `PROJECT_ID.metrics.job_log`
    (job_id, entry_number, key_date, restart_value, start_timestamp, end_timestamp, status, record_count, target_table)
    VALUES
    (p_job_kennung, p_eintrags_nr, v_datum_date, p_wiederanlauf_wert, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), v_job_status, v_record_count, 'sof_ta_e_regulierer');

EXCEPTION WHEN ERROR THEN
    SET v_job_status = 'FAILED';
    SET v_error_message = @@error.message;
    INSERT INTO `PROJECT_ID.metrics.job_log`
    (job_id, entry_number, key_date, restart_value, start_timestamp, end_timestamp, status, error_message)
    VALUES
    (p_job_kennung, p_eintrags_nr, v_datum_date, p_wiederanlauf_wert, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), v_job_status, v_error_message);
    RAISE;
END;