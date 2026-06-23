-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
CREATE OR REPLACE PROCEDURE data_operations_logs.DWMSG_AppendTimingInfos(
  IN p_eintrags_nr STRING,
  IN p_date_format STRING DEFAULT '%Y-%m-%d %H:%M:%S' -- e.g., '%Y%m%d%H%M%S'
)
BEGIN
  DECLARE v_timing_text STRING;

  IF p_eintrags_nr IS NULL THEN
    RAISE BQEXCEPTION MESSAGE 'DWMSG_AppendTimingInfos: EintragsNr must be provided.';
  END IF;

  SET v_timing_text = CONCAT(
    'Timing Info Appended: ',
    FORMAT_TIMESTAMP(p_date_format, CURRENT_TIMESTAMP())
  );

  UPDATE data_operations_logs.message_table
  SET
    zusatzinfos = CONCAT(COALESCE(zusatzinfos, ''), ' ', v_timing_text),
    updated_ts = CURRENT_TIMESTAMP()
  WHERE
    eintrags_nr = p_eintrags_nr;
END;