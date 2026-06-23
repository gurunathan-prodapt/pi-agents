-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_discount.sql
-- Description: BigQuery Stored Procedure encapsulating the data transformation logic from the original SQL script.
CREATE OR REPLACE PROCEDURE dataset.d_ausd_v_ta_p_discount(
    p_EintragsNr STRING,
    p_JobKennung STRING -- Parameter included as per design, though not explicitly used in current SQL logic.
)
BEGIN
    -- Truncate the target table before inserting new data, mirroring the legacy Oracle behavior.
    TRUNCATE TABLE dataset.ta_p_discount;

    -- Insert data into ta_p_discount
    INSERT INTO dataset.ta_p_discount (
        cntrct_id,
        disc_vector_ty,
        cntrct_obj_version,
        rabatt_alle,
        contract_number
    )
    SELECT
        da.cntrct_id,
        da.disc_vector_ty,
        da.cntrct_obj_version,
        da.rabatt_alle,
        c.contract_number
    FROM
        -- Assuming source tables ta_disc_zusgf and ta_cntrct_crs exist in the same dataset.
        -- These tables are inferred dependencies from the original SQL script.
        dataset.ta_disc_zusgf AS da
    JOIN
        dataset.ta_cntrct_crs AS c
    ON
        da.cntrct_id = c.cntrct_id
        AND da.cntrct_obj_version = c.obj_version;

    -- Note: The original script contained Oracle-specific elements such as
    -- `DEFINE`, `COLUMN s_datum new_value v_datum`, `NVL`, `spool`, `parallel hints`,
    -- `trace.sql.cfg`, `SET SERVEROUTPUT`, `WHENEVER SQLERROR`, and PL/SQL blocks
    -- (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`).
    -- These are not directly translatable or necessary in BigQuery stored procedures
    -- and have been omitted. The core DML (TRUNCATE and INSERT...SELECT) has been translated.

END;