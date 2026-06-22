-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh

-- This file contains helper BigQuery Stored Procedures for common utility functions.

-- Helper procedure for date format validation (DDMMYYYY)
CREATE OR REPLACE PROCEDURE `my-gcp-project.isbert_dataset.validate_ddmmyyyy`(
    IN p_date_string STRING,
    OUT p_date_out DATE
)
OPTIONS(
    description="Validates a date string in 'DDMMYYYY' format and converts it to a DATE type. Raises an error if invalid."
)
BEGIN
    DECLARE parsed_date DATE;
    SET parsed_date = SAFE.PARSE_DATE('%d%m%Y', p_date_string);

    IF parsed_date IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT("Invalid date format for '%s'. Expected DDMMYYYY.", p_date_string);
    ELSE
        SET p_date_out = parsed_date;
    END IF;
END;