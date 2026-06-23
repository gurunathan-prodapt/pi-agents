-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Description: DDL for processing audit table, recording summary metrics of data processing.

CREATE TABLE IF NOT EXISTS `project.dataset.processing_audit` (
    audit_id STRING NOT NULL OPTIONS(description="Unique identifier for each audit entry, typically a UUID."),
    job_id STRING NOT NULL OPTIONS(description="Foreign key to `job_control.job_id`."),
    stichtag DATE OPTIONS(description="The Stichtag (cutoff date) for which data was processed."),
    wiederanlaufwert INT64 OPTIONS(description="The restart value used for processing."),
    source_records_selected INT64 OPTIONS(description="Number of records selected from the source."),
    target_records_deleted INT64 OPTIONS(description="Number of records deleted from the target (if applicable)."),
    target_records_inserted INT64 OPTIONS(description="Number of records inserted into the target."),
    processing_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the audit entry was recorded."),
    component STRING OPTIONS(description="Component that performed the processing (e.g., 'k_ausd_bp_ta_rn_einzeln').")
)
OPTIONS(
    description="Table for auditing data processing actions and metrics."
);