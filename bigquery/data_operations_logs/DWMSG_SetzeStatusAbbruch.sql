-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
CREATE OR REPLACE PROCEDURE data_operations_logs.DWMSG_SetzeStatusAbbruch(
  IN p_eintrags_nr STRING
)
BEGIN
  IF p_eintrags_nr IS NULL THEN
    RAISE BQEXCEPTION MESSAGE 'DWMSG_SetzeStatusAbbruch: EintragsNr must be provided.';
  END IF;

  UPDATE data_operations_logs.message_table
  SET
    status = 'ABBRUCH',
    updated_ts = CURRENT_TIMESTAMP()
  WHERE
    eintrags_nr = p_eintrags_nr;
END;