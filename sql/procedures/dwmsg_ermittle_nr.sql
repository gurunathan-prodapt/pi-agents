-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Target Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

-- This stored procedure generates a unique entry number.
-- Replace `my_project.my_dataset` with your actual BigQuery project and dataset IDs.

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.dwmsg_ermittle_nr`(
    OUT p_eintrags_nr STRING
)
BEGIN
    SET p_eintrags_nr = GENERATE_UUID();
END;