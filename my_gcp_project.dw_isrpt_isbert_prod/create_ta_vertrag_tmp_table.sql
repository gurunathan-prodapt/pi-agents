-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh

-- Create the placeholder schema for the ta_vertrag_tmp table.
-- The actual schema will be refined during the migration of k_ausd_v_ta_vertrag_tmp.ksh.
CREATE TABLE IF NOT EXISTS `my_gcp_project.dw_isrpt_isbert_prod.ta_vertrag_tmp`
(
    vertrag_id STRING OPTIONS(description="Unique identifier for the contract"),
    vertrag_name STRING OPTIONS(description="Name or description of the contract"),
    vertrag_typ STRING OPTIONS(description="Type of contract"),
    gueltig_ab DATE OPTIONS(description="Start date of contract validity"),
    gueltig_bis DATE OPTIONS(description="End date of contract validity"),
    status STRING OPTIONS(description="Current status of the contract"),
    erstellungsdatum TIMESTAMP OPTIONS(description="Timestamp when the record was created"),
    letzte_aktualisierung TIMESTAMP OPTIONS(description="Timestamp of the last update to the record")
)
PARTITION BY
    DATE_TRUNC(letzte_aktualisierung, MONTH)
CLUSTER BY
    vertrag_id
OPTIONS(
    description = "Temporary table for contract data reconciliation (Vertragsdatenabgleich)."
);