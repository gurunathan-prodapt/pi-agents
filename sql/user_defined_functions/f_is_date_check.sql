-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
-- Description: BigQuery UDF to validate if a string can be parsed as a date with a given format.
-- Replaces DWDate_Datum_Check functionality.

CREATE OR REPLACE FUNCTION your_project_id.your_dataset_id.f_is_date_check(
    p_date_string STRING,
    p_format STRING
) RETURNS BOOLEAN AS
BEGIN
    RETURN SAFE.PARSE_DATE(p_format, p_date_string) IS NOT NULL;
END;