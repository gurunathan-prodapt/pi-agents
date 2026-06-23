-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
CREATE OR REPLACE PROCEDURE data_operations_logs.DWMSG_ErmittleNr(
  OUT p_eintrags_nr STRING
)
BEGIN
  SET p_eintrags_nr = GENERATE_UUID();
END;