-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
CREATE OR REPLACE PROCEDURE data_operations_logs.DWMSG_ErzeugeEintrag(
  IN p_eintrags_nr STRING,
  IN p_job_kennung STRING,
  IN p_programmname STRING,
  IN p_logdatei STRING
)
BEGIN
  IF p_eintrags_nr IS NULL OR p_job_kennung IS NULL OR p_programmname IS NULL OR p_logdatei IS NULL THEN
    RAISE BQEXCEPTION MESSAGE 'DWMSG_ErzeugeEintrag: All input parameters must be provided.';
  END IF;

  INSERT INTO data_operations_logs.message_table (
    eintrags_nr,
    job_kennung,
    programmname,
    logdatei,
    status,
    created_ts,
    updated_ts
  )
  VALUES (
    p_eintrags_nr,
    p_job_kennung,
    p_programmname,
    p_logdatei,
    'OPEN',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );
END;