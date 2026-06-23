-- DDL for project_id.dataset_id.fos_vertrags_cache
-- Target table for the processed contract cache data, migrated from legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh
CREATE TABLE IF NOT EXISTS `project_id.dataset_id.fos_vertrags_cache` (
    dwh_vertrag_id INT64 NOT NULL,
    vertrags_nummer STRING NOT NULL,
    gueltig_von DATE NOT NULL,
    gueltig_bis DATE NOT NULL,
    betrag NUMERIC,
    ladedatum DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);