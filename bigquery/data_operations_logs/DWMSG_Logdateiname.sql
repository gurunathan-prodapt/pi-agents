-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
CREATE OR REPLACE PROCEDURE data_operations_logs.DWMSG_Logdateiname(
  IN p_job_kennung STRING,
  IN p_eintrags_nr STRING,
  OUT p_logdateiname STRING
)
BEGIN
  -- Assuming DW_DIR_PROT is a configurable constant, here hardcoded for example.
  -- In a real scenario, this would likely come from a config table or another parameter.
  DECLARE v_dw_dir_prot STRING DEFAULT '/var/log/isbert/';

  SET p_logdateiname = CONCAT(
    v_dw_dir_prot,
    p_job_kennung,
    '_',
    FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()),
    '_',
    p_eintrags_nr,
    '.log'
  );
END;