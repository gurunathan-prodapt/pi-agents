-- DDL for table `pds.ta_bpri_com`
-- Inferred from usage in d_ausd_bp_ta_bpr_basis_his.sql
-- Original job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

CREATE TABLE IF NOT EXISTS `your-gcp-project.pds.ta_bpri_com` (
    cntrct_id STRING NOT NULL,
    bpr_id INT64 NOT NULL,
    bpri_com_id STRING,
    iccid_mi STRING,
    iccid_ii STRING,
    iccid_iai STRING,
    iccid_nr STRING,
    iccid_cd STRING,
    imsi_mcc STRING,
    imsi_mnc STRING,
    imsi_hlr STRING,
    imsi_si STRING,
    cntrct_id_ref STRING,
    valid_from DATE,
    valid_to DATE,
    modified_at DATE,
    insert_at DATE,
    slave_number STRING,
    eid STRING
    -- Add other columns as per actual source schema
);