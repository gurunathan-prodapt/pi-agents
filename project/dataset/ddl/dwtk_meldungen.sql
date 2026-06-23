--
-- Target BigQuery DDL for table project.dataset.dwtk_meldungen
-- Replaces Oracle table isbert_schema.dwtk_meldungen.
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh
--
-- Note: Placeholder column `message_text` is added as the full source schema
-- was not provided in the design document.
--
CREATE TABLE IF NOT EXISTS project.dataset.dwtk_meldungen (
    message_id STRING NOT NULL OPTIONS(description="Unique identifier for the message"),
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job that created the message"),
    timecreated TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the message was created"),
    message_text STRING OPTIONS(description="Content of the message")
)
PARTITION BY
    DATE(timecreated)
CLUSTER BY
    job_kennung;