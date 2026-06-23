-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_geschaeftspartner.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh

CREATE TABLE IF NOT EXISTS isbert_target_ds.sof_ta_p_evn_empf (
    CNTRCT_ID STRING,
    NAMENSZUSATZ STRING,
    ADRESSZUSATZ STRING,
    FIRMENNAME STRING,
    AKAD_TITEL STRING,
    NACHNAME STRING,
    VORNAME STRING,
    LAND STRING,
    PLZ STRING,
    WOHNORT STRING,
    STRASSE STRING,
    ORGANISATIONSEINHEIT STRING,
    MWST_KENNZEICHEN STRING
);