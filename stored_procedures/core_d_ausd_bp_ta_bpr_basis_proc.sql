-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_basis.sql
-- Migrated from job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh
-- Description: BigQuery Stored Procedure encapsulating the core data transformation logic.

CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_orchestration_dataset.core_d_ausd_bp_ta_bpr_basis_proc`(
    p_stichtag_str STRING,
    p_source_project_id STRING,
    p_source_dataset_id STRING,
    p_staging_project_id STRING,
    p_staging_dataset_id STRING
)
OPTIONS(strict_mode=true)
BEGIN

    -- Dynamic table references
    DECLARE v_rma_ta_sim_fqdn STRING DEFAULT CONCAT('`', p_source_project_id, '.', p_source_dataset_id, '.rma_ta_sim`');
    DECLARE v_rma_ta_sim_card_type_fqdn STRING DEFAULT CONCAT('`', p_source_project_id, '.', p_source_dataset_id, '.rma_ta_sim_card_type`');
    DECLARE v_sof_ta_sim_fqdn STRING DEFAULT CONCAT('`', p_staging_project_id, '.', p_staging_dataset_id, '.sof_ta_sim`');
    DECLARE v_sof_ta_bpr_basis_fqdn STRING DEFAULT CONCAT('`', p_staging_project_id, '.', p_staging_dataset_id, '.sof_ta_bpr_basis`');
    DECLARE v_sof_ta_bpr_basis_his_fqdn STRING DEFAULT CONCAT('`', p_staging_project_id, '.', p_staging_dataset_id, '.sof_ta_bpr_basis_his`');

    -- ========================= Step01 ==================================
    -- Clearing temporary tables.
    EXECUTE IMMEDIATE CONCAT('TRUNCATE TABLE ', v_sof_ta_sim_fqdn);
    EXECUTE IMMEDIATE CONCAT('TRUNCATE TABLE ', v_sof_ta_bpr_basis_fqdn);

    -- ========================= Step02 ==================================
    -- Create local copy of the latest SIMs
    EXECUTE IMMEDIATE CONCAT('''
        INSERT INTO ', v_sof_ta_sim_fqdn, '
        (
         iccid,
         sim_card_type_id,
         card_type_name
        )
        SELECT
            CONCAT(sim.iccid_mi, '-', sim.iccid_ii, '-', sim.iccid_iai, '-', sim.iccid_nr, '-', sim.iccid_cd) AS iccid,
            sim.sim_card_type_id,
            card.card_type_name
          FROM
               ', v_rma_ta_sim_fqdn, ' AS sim
          JOIN ', v_rma_ta_sim_card_type_fqdn, ' AS card
            ON card.sim_card_type_id = sim.sim_card_type_id
          WHERE
            sim.insert_at     <= PARSE_DATE(''%Y%m%d'', ''', p_stichtag_str, ''')
            AND ( sim.modified_at   IS NULL
                OR sim.modified_at   > PARSE_DATE(''%Y%m%d'', ''', p_stichtag_str, ''') )
            AND sim.valid_from      <= PARSE_DATE(''%Y%m%d'', ''', p_stichtag_str, ''')
            AND ( sim.valid_to      IS NULL
                OR sim.valid_to      > PARSE_DATE(''%Y%m%d'', ''', p_stichtag_str, ''') )
            AND card.insert_at   <= PARSE_DATE(''%Y%m%d'', ''', p_stichtag_str, ''')
            AND ( card.modified_at IS NULL
                OR card.modified_at     > PARSE_DATE(''%Y%m%d'', ''', p_stichtag_str, ''') )
    ''');

    -- ========================= Step03 ==================================
    -- Create local copy of the latest basis product instances for tnv,tc, tb, da, vda, tk ms
    EXECUTE IMMEDIATE CONCAT('''
        INSERT INTO ', v_sof_ta_bpr_basis_fqdn, '
        ( cntrct_id,
          bpr_id,
          bpr_instance_id,
          iccid,
          imsi_mcc,
          imsi_mnc,
          imsi_hlr,
          imsi_si,
          valid_to,
          slave_number,
          e_id,
          card_type_name )
        SELECT
                bp.cntrct_id,
                bp.bpr_id,
                bp.bpri_com_id   AS bpr_instance_id,
                bp.iccid,
                bp.imsi_mcc,
                bp.imsi_mnc,
                bp.imsi_hlr,
                bp.imsi_si,
                COALESCE(bp.valid_to, DATE ''4712-12-31'') AS valid_to,
                bp.slave_number,
                bp.e_id,
                sim.card_type_name
        FROM
            (
                SELECT
                    bp1.*,
                    MAX( COALESCE(bp1.valid_to, DATE ''4712-12-31'') )
                        OVER ( PARTITION BY bp1.cntrct_id, bp1.bpr_id ) AS max_valid_to
                FROM ', v_sof_ta_bpr_basis_his_fqdn, ' AS bp1
            ) AS bp
        LEFT JOIN ', v_sof_ta_sim_fqdn, ' AS sim
            ON bp.iccid = sim.iccid
        WHERE
            COALESCE(bp.valid_to, DATE ''4712-12-31'') = bp.max_valid_to
    ''');

END;