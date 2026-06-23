-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
CREATE OR REPLACE PROCEDURE data_operations_logs.DWMSG_SetzeStichtagInfo(
  IN p_eintrags_nr STRING,
  IN p_stichtag STRING,     -- Date string
  IN p_stichtag_fmt STRING  -- Date format, e.g., '%Y%m%d'
)
BEGIN
  DECLARE v_formatted_stichtag STRING;

  IF p_eintrags_nr IS NULL OR p_stichtag IS NULL OR p_stichtag_fmt IS NULL THEN
    RAISE BQEXCEPTION MESSAGE 'DWMSG_SetzeStichtagInfo: EintragsNr, Stichtag, and StichtagFmt must be provided.';
  END IF;

  SET v_formatted_stichtag = FORMAT_DATE('%Y-%m-%d', PARSE_DATE(p_stichtag_fmt, p_stichtag));

  UPDATE data_operations_logs.message_table
  SET
    zusatzinfos = CONCAT(COALESCE(zusatzinfos, ''), ' Stichtag: ', v_formatted_stichtag),
    updated_ts = CURRENT_TIMESTAMP()
  WHERE
    eintrags_nr = p_eintrags_nr;
END;