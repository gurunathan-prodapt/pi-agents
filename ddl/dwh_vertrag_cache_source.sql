-- DDL for project_id.dataset_id.dwh_vertrag_cache_source
-- Source table for the contract cache data, referenced by the core logic migrated from legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh
-- This table is assumed to exist and be populated.
CREATE TABLE IF NOT EXISTS `project_id.dataset_id.dwh_vertrag_cache_source` (
    dwh_vertrag_id INT64 NOT NULL,
    vertrags_nummer STRING NOT NULL,
    gueltig_von DATE NOT NULL,
    gueltig_bis DATE NOT NULL,
    betrag NUMERIC,
    ladedatum DATE NOT NULL,
    source_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);