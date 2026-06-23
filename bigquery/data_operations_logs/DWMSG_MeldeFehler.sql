-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
CREATE OR REPLACE PROCEDURE data_operations_logs.DWMSG_MeldeFehler(
  IN p_eintrags_nr STRING,
  IN p_typ STRING,         -- F (Fatal), E (Error), W (Warning)
  IN p_fehler_nr INT64,
  IN p_zusatz1 STRING,
  IN p_zusatz2 STRING,
  IN p_zusatzinfos STRING
)
BEGIN
  IF p_eintrags_nr IS NULL OR p_typ IS NULL OR p_fehler_nr IS NULL THEN
    RAISE BQEXCEPTION MESSAGE 'DWMSG_MeldeFehler: EintragsNr, Typ, and FehlerNr must be provided.';
  END IF;

  UPDATE data_operations_logs.message_table
  SET
    fehler_typ = p_typ,
    fehler_nr = p_fehler_nr,
    zusatz1 = COALESCE(p_zusatz1, zusatz1), -- Update if provided, else keep existing
    zusatz2 = COALESCE(p_zusatz2, zusatz2), -- Update if provided, else keep existing
    zusatzinfos = COALESCE(p_zusatzinfos, zusatzinfos), -- Update if provided, else keep existing
    updated_ts = CURRENT_TIMESTAMP()
  WHERE
    eintrags_nr = p_eintrags_nr;
END;